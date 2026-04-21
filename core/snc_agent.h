#include <jni.h>
#ifndef SNC_AGENT_H
#define SNC_AGENT_H

extern JNIEnv *sncAgentJNIEnv; // 声明全局JNIEnv指针

// 声明 sncAgentSend 函数
void sncAgentSend();

#endif // SNC_AGENT_H