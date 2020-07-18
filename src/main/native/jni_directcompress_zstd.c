#include <jni.h>
#include <zstd_internal.h>
#include <zstd_errors.h>
#include <stdlib.h>
#include <stdint.h>

/* field IDs can't change in the same VM */
static jfieldID consumed_id;
static jfieldID produced_id;

/*
 * Class:     com_github_luben_zstd_ZstdDirectCompressingStream
 * Method:    recommendedCOutSize
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_com_github_luben_zstd_ZstdDirectCompressingStream_recommendedCOutSize
  (JNIEnv *env, jclass obj) {
    return (jint) ZSTD_CStreamOutSize();
}

/*
 * Class:     com_github_luben_zstd_ZstdDirectCompressingStream
 * Method:    createCStream
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_com_github_luben_zstd_ZstdDirectCompressingStream_createCStream
  (JNIEnv *env, jclass obj) {
    return (jlong)(intptr_t) ZSTD_createCStream();
}

/*
 * Class:     com_github_luben_zstd_ZstdDirectCompressingStream
 * Method:    freeCStream
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_com_github_luben_zstd_ZstdDirectCompressingStream_freeCStream
  (JNIEnv *env, jclass obj, jlong stream) {
    return ZSTD_freeCStream((ZSTD_CStream *)(intptr_t) stream);
}

/*
 * Class:     com_github_luben_zstd_ZstdDirectCompressingStream
 * Method:    initCStream
 * Signature: (JI)I
 */
JNIEXPORT jint JNICALL Java_com_github_luben_zstd_ZstdDirectCompressingStream_initCStream
  (JNIEnv *env, jclass obj, jlong stream, jint level) {
    jclass clazz = (*env)->GetObjectClass(env, obj);
    consumed_id = (*env)->GetFieldID(env, clazz, "consumed", "I");
    produced_id = (*env)->GetFieldID(env, clazz, "produced", "I");
    return ZSTD_initCStream((ZSTD_CStream *)(intptr_t) stream, level);
}

/*
 * Class:     com_github_luben_zstd_ZstdDirectCompressingStream
 * Method:    initCStreamWithDict
 * Signature: (J[BII)I
 */
JNIEXPORT jint JNICALL Java_com_github_luben_zstd_ZstdDirectCompressingStream_initCStreamWithDict
  (JNIEnv *env, jclass obj, jlong stream, jbyteArray dict, jint dict_size, jint level) {
    size_t result = (size_t)(0-ZSTD_error_memory_allocation);
    jclass clazz = (*env)->GetObjectClass(env, obj);
    consumed_id =                                    (*env)->GetFieldID(env, clazz, "consumed", "I");
    produced_id = (*env)->GetFieldID(env, clazz, "produced", "I");
    void *dict_buff = (*env)->GetPrimitiveArrayCritical(env, dict, NULL);
    if (dict_buff == NULL) goto E1;
    result = ZSTD_initCStream_usingDict((ZSTD_CStream *)(intptr_t) stream, dict_buff, dict_size, level);
    (*env)->ReleasePrimitiveArrayCritical(env, dict, dict_buff, JNI_ABORT);
E1:
    return result;
}

/*
 * Class:     com_github_luben_zstd_ZstdDirectCompressingStream
 * Method:    initCStreamWithFastDict
 */
JNIEXPORT jint JNICALL Java_com_github_luben_zstd_ZstdDirectCompressingStream_initCStreamWithFastDict
  (JNIEnv *env, jclass obj, jlong stream, jobject dict) {
    jclass clazz = (*env)->GetObjectClass(env, obj);
    consumed_id = (*env)->GetFieldID(env, clazz, "consumed", "I");
    produced_id = (*env)->GetFieldID(env, clazz, "produced", "I");
    jclass dict_clazz = (*env)->GetObjectClass(env, dict);
    jfieldID compress_dict = (*env)->GetFieldID(env, dict_clazz, "nativePtr", "J");
    ZSTD_CDict* cdict = (ZSTD_CDict*)(intptr_t)(*env)->GetLongField(env, dict, compress_dict);
    if (cdict == NULL) return ZSTD_error_dictionary_wrong;
    return ZSTD_initCStream_usingCDict((ZSTD_CStream *)(intptr_t) stream, cdict);
}

/*
 * Class:     com_github_luben_zstd_ZstdDirectCompressingStream
 * Method:    compressDirectByteBuffer
 * Signature: (JLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;II)J
 */
JNIEXPORT jlong JNICALL Java_com_github_luben_zstd_ZstdDirectCompressingStream_compressDirectByteBuffer
  (JNIEnv *env, jclass obj, jlong stream, jlong dst_buf, jint dst_offset, jint dst_size, jlong src_buf, jint src_offset, jint src_size) {

    size_t size = (size_t)ERROR(memory_allocation);
    char *dst_buf_ptr = (char*)(*env)dst_buf;
    if (dst_buf_ptr == NULL) goto E1;
    char *src_buf_ptr = (char*)(*env)src_buf;
    if (src_buf_ptr == NULL) goto E1;

    ZSTD_outBuffer output = { dst_buf_ptr + dst_offset, dst_size, 0 };
    ZSTD_inBuffer input = { src_buf_ptr + src_offset, src_size, 0 };

    size = ZSTD_compressStream((ZSTD_CStream *)(intptr_t) stream, &output, &input);

    (*env)->SetIntField(env, obj, consumed_id, input.pos);
    (*env)->SetIntField(env, obj, produced_id, output.pos);
E1: return size;
}

/*
 * Class:     com_github_luben_zstd_ZstdDirectCompressingStream
 * Method:    endStream
 * Signature: (JLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL Java_com_github_luben_zstd_ZstdDirectCompressingStream_endStream
  (JNIEnv *env, jclass obj, jlong stream, jlong dst_buf, jint dst_offset, jint dst_size) {

    size_t size = (size_t)(0-ZSTD_error_memory_allocation);

    char *dst_buf_ptr = (char*)(*env)(dst_buf);

    if (dst_buf_ptr != NULL) {
        ZSTD_outBuffer output = { dst_buf_ptr + dst_offset, dst_size, 0 };
        size = ZSTD_endStream((ZSTD_CStream *)(intptr_t) stream, &output);
        (*env)->SetIntField(env, obj, produced_id, output.pos);
    }
    return (jint) size;
}

/*
 * Class:     com_github_luben_zstd_ZstdDirectCompressingStream
 * Method:    flushStream
 * Signature: (JLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL Java_com_github_luben_zstd_ZstdDirectCompressingStream_flushStream
  (JNIEnv *env, jclass obj, jlong stream, jlong dst_buf, jint dst_offset, jint dst_size) {

    size_t size = (size_t)(0-ZSTD_error_memory_allocation);
    char *dst_buf_ptr = (char*)(*env)dst_buf;
    if (dst_buf_ptr != NULL) {
        ZSTD_outBuffer output = { dst_buf_ptr + dst_offset, dst_size, 0 };
        size = ZSTD_flushStream((ZSTD_CStream *)(intptr_t) stream, &output);
        (*env)->SetIntField(env, obj, produced_id, output.pos);
    }
    return (jint) size;
}
