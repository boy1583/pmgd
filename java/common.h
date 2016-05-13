#ifndef JAVA_COMMON_H
#define JAVA_COMMON_H

#include <jni.h>
#include "jarvis.h"
#include "../src/compiler.h"

extern THREAD jobject java_transaction;

template <typename T>
inline T *getJarvisHandle(JNIEnv *env, jobject obj)
{
    static jfieldID id = 0;
    if (id == 0) {
        jclass cls = env->GetObjectClass(obj);
        id = env->GetFieldID(cls, "jarvisHandle", "J");
    }
    jlong handle = env->GetLongField(obj, id);
    return reinterpret_cast<T *>(handle);
}

template <typename T>
inline void setJarvisHandle(JNIEnv *env, jobject obj, T *t)
{
    static jfieldID id = 0;
    if (id == 0) {
        jclass cls = env->GetObjectClass(obj);
        id = env->GetFieldID(cls, "jarvisHandle", "J");
    }
    jlong handle = reinterpret_cast<jlong>(t);
    env->SetLongField(obj, id, handle);
}

extern jobject new_java_node(JNIEnv *env, Jarvis::Node &);
extern jobject new_java_edge(JNIEnv *env, Jarvis::Edge &);
extern jobject new_java_property(JNIEnv *env, Jarvis::Property *);
extern jobject new_java_stringid(JNIEnv *env, Jarvis::StringID);
extern jobject java_node_iterator(JNIEnv *env, Jarvis::NodeIterator &&);
extern jobject java_edge_iterator(JNIEnv *env, Jarvis::EdgeIterator &&);
extern jobject java_property_iterator(JNIEnv *env, Jarvis::PropertyIterator &&);
extern void JavaThrow(JNIEnv *env, Jarvis::Exception e);

#endif
