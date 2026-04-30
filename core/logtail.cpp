// Copyright 2022 iLogtail Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/wait.h>
#ifndef LOGTAIL_NO_TC_MALLOC
// #include <gperftools/heap-profiler.h> // for memory leak debug
#endif
#include "app_config/AppConfig.h"
#include "application/Application.h"
#include "common/ErrorUtil.h"
#include "common/Flags.h"
#include "common/version.h"
#include "logger/Logger.h"
#include <iostream>
#include <jni.h>
#include <string>
#include <sstream>
#include "com_shsnc_agent_ivory_plugin_loongcollector_LoongCollectorProcessor.h"
#include "snc_agent.h"

using namespace logtail;

// ============================
//  SHSNC JNI 全局变量（必须放在最前面）
// ============================
static JavaVM* g_jvm = nullptr;
static std::mutex g_jni_call_mutex;

// ✨ 新增：缓存Java类和方法ID
static jclass g_loongcollector_processor_class = nullptr;
static jmethodID g_send_message_method = nullptr;
static std::once_flag g_jni_init_flag;

#ifdef ENABLE_COMPATIBLE_MODE
extern "C" {
#include <string.h>
asm(".symver memcpy, memcpy@GLIBC_2.2.5");
void* __wrap_memcpy(void* dest, const void* src, size_t n) {
    return memcpy(dest, src, n);
}
}
#endif

DECLARE_FLAG_BOOL(ilogtail_disable_core);
DECLARE_FLAG_INT32(max_open_files_limit);
DECLARE_FLAG_INT32(max_reader_open_files);
DECLARE_FLAG_STRING(logtail_sys_conf_dir);
DECLARE_FLAG_STRING(check_point_filename);
DECLARE_FLAG_STRING(default_buffer_file_path);
DECLARE_FLAG_STRING(ilogtail_docker_file_path_config);
DECLARE_FLAG_STRING(metrics_report_method);
DECLARE_FLAG_INT32(data_server_port);
DECLARE_FLAG_BOOL(enable_env_ref_in_config);
DECLARE_FLAG_BOOL(enable_sls_metrics_format);
DECLARE_FLAG_BOOL(logtail_mode);


void HandleSigtermSignal(int signum, siginfo_t* info, void* context) {
    LOG_INFO(sLogger, ("received signal", "SIGTERM"));
    Application::GetInstance()->SetSigTermSignalFlag(true);
}

void disable_core(void) {
    struct rlimit rlim;
    rlim.rlim_cur = rlim.rlim_max = 0;
    setrlimit(RLIMIT_CORE, &rlim);
}

void enable_core(void) {
    struct rlimit rlim_old;
    struct rlimit rlim_new;
    if (getrlimit(RLIMIT_CORE, &rlim_old) == 0) {
        rlim_new.rlim_cur = rlim_new.rlim_max = 1024 * 1024 * 1024;
        if (setrlimit(RLIMIT_CORE, &rlim_new) != 0) {
            rlim_new.rlim_cur = rlim_old.rlim_cur;
            rlim_new.rlim_max = rlim_old.rlim_max;
            (void)setrlimit(RLIMIT_CORE, &rlim_new);
        }
    }
}

static void overwrite_community_edition_flags() {
    // support run in installation dir on default
    if (BOOL_FLAG(logtail_mode)) {
        STRING_FLAG(logtail_sys_conf_dir) = ".";
        STRING_FLAG(check_point_filename) = "checkpoint/logtail_check_point";
        STRING_FLAG(default_buffer_file_path) = "checkpoint";
        STRING_FLAG(ilogtail_docker_file_path_config) = "checkpoint/docker_path_config.json";
    }
    STRING_FLAG(metrics_report_method) = "";
    INT32_FLAG(data_server_port) = 443;
    BOOL_FLAG(enable_env_ref_in_config) = true;
    BOOL_FLAG(enable_sls_metrics_format) = false;
}

// Main routine of worker process.
void do_worker_process() {
    CreateAgentDir();

    Logger::Instance().InitGlobalLoggers();

    struct sigaction sigtermSig;
    sigemptyset(&sigtermSig.sa_mask);
    sigtermSig.sa_sigaction = HandleSigtermSignal;
    sigtermSig.sa_flags = SA_SIGINFO;
    if (sigaction(SIGTERM, &sigtermSig, NULL) < 0) {
        LOG_ERROR(sLogger, ("install SIGTERM", "fail"));
        exit(5);
    }
    if (sigaction(SIGINT, &sigtermSig, NULL) < 0) {
        LOG_ERROR(sLogger, ("install SIGINT", "fail"));
        exit(5);
    }

#ifndef LOGTAIL_NO_TC_MALLOC
    if (BOOL_FLAG(ilogtail_disable_core)) {
        disable_core();
    } else {
        enable_core();
    }
#else
    enable_core();
#endif

    overwrite_community_edition_flags();

    // set max open file limit
    struct rlimit rlimMaxOpenFiles;
    rlimMaxOpenFiles.rlim_cur = rlimMaxOpenFiles.rlim_max = INT32_FLAG(max_open_files_limit);
    if (0 != setrlimit(RLIMIT_NOFILE, &rlimMaxOpenFiles)) {
        LOG_ERROR(sLogger,
                  ("set resource limit error, open file limit",
                   INT32_FLAG(max_open_files_limit))("reason", ErrnoToString(GetErrno())));
        if (getrlimit(RLIMIT_NOFILE, &rlimMaxOpenFiles) == 0) {
            LOG_ERROR(sLogger,
                      ("this process's resource limit ", rlimMaxOpenFiles.rlim_cur)("max", rlimMaxOpenFiles.rlim_max));
            if (rlimMaxOpenFiles.rlim_max > (rlim_t)INT32_FLAG(max_open_files_limit)) {
                rlimMaxOpenFiles.rlim_max = INT32_FLAG(max_open_files_limit);
            }
            if (rlimMaxOpenFiles.rlim_max < (rlim_t)100) {
                rlimMaxOpenFiles.rlim_max = 100;
            }
            INT32_FLAG(max_open_files_limit) = rlimMaxOpenFiles.rlim_max;
            INT32_FLAG(max_reader_open_files) = (int32_t)(INT32_FLAG(max_open_files_limit) * 0.8);
        } else {
            LOG_ERROR(sLogger,
                      ("get resource limit error, "
                       "set max open files to 800",
                       ErrnoToString(GetErrno())));
            INT32_FLAG(max_open_files_limit) = 1024;
            INT32_FLAG(max_reader_open_files) = (int32_t)(1024 * 0.8);
        }
    }

    Application::GetInstance()->Init();
    Application::GetInstance()->Start();
}

int main(int argc, char** argv) {
    gflags::SetUsageMessage(
        std::string("The Lightweight Collector of SLS in Alibaba Cloud\nUsage: ./ilogtail [OPTION]"));
    gflags::SetVersionString(std::string(ILOGTAIL_VERSION) + " Community Edition");
    if (argc != 0) {
        google::ParseCommandLineFlags(&argc, &argv, true);
    }

    if (setenv("TCMALLOC_RELEASE_RATE", "10.0", 1) == -1) {
        exit(3);
    }

    // HeapProfilerStart("my_heap_profile"); // for memory leak debug
    do_worker_process();

    return 0;
}

// ============================
//  JNI 方法
// ============================
JNIEXPORT void JNICALL Java_com_shsnc_agent_ivory_plugin_loongcollector_LoongCollectorProcessor_loogcollectorStart(JNIEnv *env, jclass clazz, jstring homePath) {
    std::cout << "LoongcollectorPlugin started!" << std::endl;

    // 保存 JavaVM（全局唯一线程安全对象）
    env->GetJavaVM(&g_jvm);
    std::cout << "初始化全局 g_jvm 成功" << std::endl;

    jboolean isCopy;
    const char *basePath = env->GetStringUTFChars(homePath, &isCopy); //UTF-8

    std::string workDir = "--work_dir=" + std::string(basePath);
    std::string conf = "--conf_dir=" + std::string(basePath) + "/conf";
    std::string logs = "--logs_dir=" + std::string(basePath) + "/logs";
    std::string data = "--data_dir=" + std::string(basePath) + "/data";
    std::string run = "--run_dir=" + std::string(basePath) + "/run";
    std::string third_party = "--third_party_dir=" + std::string(basePath) + "/third_party";

    const char* arg_array[] = {
        "./loongcollector",  // 程序名
        workDir.c_str(),  // 工作目录参数
        conf.c_str(),  // 配置目录参数
        logs.c_str(),  // 日志目录参数
        data.c_str(),  // 数据目录参数
        run.c_str(),  // 运行目录参数
        third_party.c_str(),
        NULL       // 必须
    };
    int argc = sizeof(arg_array)/sizeof(arg_array[0]) - 1;
    char** argv = const_cast<char**>(arg_array);

    std::cout << "命令行参数列表: argc=" << argc << std::endl;
    for (size_t i = 0; arg_array[i] != NULL; i++)
    {
        std::cout << "arg[" << i << "]: " << arg_array[i] << std::endl;
    }

    main(argc, argv);
    std::cout << "Loongcollector main started" << std::endl;
}

JNIEXPORT void JNICALL Java_com_shsnc_agent_ivory_plugin_loongcollector_LoongCollectorProcessor_loogcollectorStop(JNIEnv *, jclass){
    std::cout << "LoongcollectorProcessor stopped!" << std::endl;
    Application::GetInstance()->SetSigTermSignalFlag(true);
}

// ============================
//  获取线程安全的 JNIEnv
//  SHSNC JNI 线程安全终极版
//  解决：SIGSEGV、栈溢出、跨线程崩溃
// ============================
static JNIEnv* getThreadJNIEnv() {
    if (!g_jvm) {
        std::cerr << "[SHSNC-JNI] g_jvm is null" << std::endl;
        return nullptr;
    }

    JNIEnv* env = nullptr;
    jint result = g_jvm->GetEnv((void**)&env, JNI_VERSION_1_6);

    if (result == JNI_EDETACHED) {
        JavaVMAttachArgs args;
        args.version = JNI_VERSION_1_6;
        args.name = (char*)"shsnc-agent-sender";
        args.group = nullptr;

        // 修复类型强转问题
        jint attach_ret = g_jvm->AttachCurrentThread((void**)&env, &args);
        if (attach_ret != 0) {
            std::cerr << "[SHSNC-JNI] AttachCurrentThread failed: " << attach_ret << std::endl;
            return nullptr;
        }
    }
    return env;
}

// ============================
//  JNI 缓存初始化
// ============================
static void initJNICache(JNIEnv* env) {
    std::call_once(g_jni_init_flag, [env]() {
        if (!env) {
            std::cerr << "[SHSNC-JNI] initJNICache: env is null" << std::endl;
            return;
        }

        // 1. 查找类并转换为全局引用
        jclass localCls = env->FindClass("com/shsnc/agent/ivory/plugin/loongcollector/LoongCollectorProcessor");
        if (!localCls || env->ExceptionCheck()) {
            std::cerr << "[SHSNC-JNI] initJNICache: FindClass failed" << std::endl;
            env->ExceptionClear();
            return;
        }

        g_loongcollector_processor_class = static_cast<jclass>(env->NewGlobalRef(localCls));
        env->DeleteLocalRef(localCls);

        if (!g_loongcollector_processor_class) {
            std::cerr << "[SHSNC-JNI] initJNICache: NewGlobalRef failed" << std::endl;
            return;
        }

        std::cout << "[SHSNC-JNI] Created global ref for LoongcollectorProcessor class" << std::endl;

        // 2. 获取方法ID并缓存
        g_send_message_method = env->GetStaticMethodID(
            g_loongcollector_processor_class, 
            "sncAgentSendMessage", 
            "([B)V"
        );

        if (!g_send_message_method || env->ExceptionCheck()) {
            std::cerr << "[SHSNC-JNI] initJNICache: GetStaticMethodID failed" << std::endl;
            env->ExceptionClear();
            if (g_loongcollector_processor_class) {
                env->DeleteGlobalRef(g_loongcollector_processor_class);
                g_loongcollector_processor_class = nullptr;
            }
            return;
        }

        std::cout << "[SHSNC-JNI] Cached method ID for sncAgentSendMessage" << std::endl;
    });
}

// ============================
//  JNI 缓存清理
// ============================
// static void cleanupJNICache(JNIEnv* env) {
//     if (!env) {
//         return;
//     }

//     if (g_loongcollector_processor_class) {
//         env->DeleteGlobalRef(g_loongcollector_processor_class);
//         g_loongcollector_processor_class = nullptr;
//     }

//     g_send_message_method = nullptr;
//     std::cout << "[SHSNC-JNI] Cleaned up JNI cache" << std::endl;
// }

// ============================
//  线程安全发送（给Go调用）
// ============================
#ifdef __cplusplus
extern "C" {
#endif

void sncAgentSendData(const char* data, int dataLen) {
    if (!g_jvm || !data || dataLen <= 0) {
        return;
    }

    //std::cout << "[SHSNC-JNI] sncAgentSendData called, dataLen=" << dataLen << std::endl;

    std::lock_guard<std::mutex> lock(g_jni_call_mutex);
    JNIEnv* env = getThreadJNIEnv();
    if (!env) {
        return;
    }

    //std::cout << "[SHSNC-JNI] Obtained JNIEnv for current thread" << std::endl;

    // 初始化缓存（只执行一次）
    initJNICache(env);

        // 检查缓存是否有效
    if (!g_loongcollector_processor_class || !g_send_message_method) {
        std::cerr << "[SHSNC-JNI] sncAgentSendData: JNI cache not initialized" << std::endl;
        return;
    }

    jbyteArray j_bytes = nullptr;

    try {
        // 创建字节数组
        j_bytes = env->NewByteArray(dataLen);
        if (!j_bytes || env->ExceptionCheck()) {
            env->ExceptionClear();
            return;
        }
        env->SetByteArrayRegion(j_bytes, 0, dataLen, (const jbyte*)data);

        //std::cout << "[SHSNC-JNI] Created Java byte array for data, len=" << dataLen << std::endl;

        // 调用Java方法（使用缓存的类和方法ID）
        env->CallStaticVoidMethod(g_loongcollector_processor_class, g_send_message_method, j_bytes);
        if (env->ExceptionCheck()) {
            env->ExceptionClear();
        }

        //std::cout << "[SHSNC-JNI] Called Java method sncAgentSendMessage" << std::endl;

    } catch (...) {
        if (env->ExceptionCheck()) {
            env->ExceptionClear();
        }
    }

    // 统一释放引用（关键：防止内存泄漏）
    if (j_bytes) env->DeleteLocalRef(j_bytes);

    //std::cout << "[SHSNC-JNI] Finished sncAgentSendData" << std::endl;
}

#ifdef __cplusplus
}
#endif