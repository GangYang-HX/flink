// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: kfc.proto

package ai.kfc;

public final class Kfc {
  private Kfc() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    String[] descriptorData = {
      "\n\tkfc.proto\022\006ai.kfc\032\021kfc_message.proto2;" +
      "\n\003KFC\0224\n\tGetValues\022\022.ai.kfc.KFCRequest\032\023" +
      ".ai.kfc.KFCResponseb\006proto3"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          KfcMessage.getDescriptor(),
        }, assigner);
    KfcMessage.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
