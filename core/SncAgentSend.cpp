#include <iostream>
#include <jni.h>
#include "snc_agent.h"
#include <sstream>

// 线程函数
void sncAgentSend() {
    std::cout << "子线程运行\n";
    if (sncAgentJNIEnv != nullptr)
    {
        // 调用Java方法
        jclass clazz = sncAgentJNIEnv->FindClass("com/shsnc/agent/plugin/loongcollector/LoongcollectorPlugin");
        if (clazz == nullptr) {
            std::cerr << "Failed to find class com/shsnc/agent/plugin/loongcollector/LoongcollectorPlugin" << std::endl;
            return;
        }
        jmethodID staticMethodID = sncAgentJNIEnv->GetStaticMethodID(clazz, "sncAgentSendMessage","(Ljava/lang/String;)V");
        if (staticMethodID == nullptr) {
            std::cerr << "Failed to find method sncAgentSendMessage" << std::endl;
            return;
        }
        for (int i = 0; i < 5; ++i) {
            std::stringstream ss;
            ss << "Message from native code: " << i;
            jstring message = sncAgentJNIEnv->NewStringUTF(ss.str().c_str());
            sncAgentJNIEnv->CallStaticVoidMethod(clazz, staticMethodID, message);
            sncAgentJNIEnv->DeleteLocalRef(message);
        }
    } else {
        std::cerr << "sncAgentJNIEnv is null!" << std::endl;
    }
    std::cout << "子线程结束\n";
}
