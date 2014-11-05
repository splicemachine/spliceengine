package com.splicemachine.coprocessor;

public final class SpliceMessage {
  private SpliceMessage() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface SpliceSchedulerRequestOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // optional bytes taskStart = 1;
    /**
     * <code>optional bytes taskStart = 1;</code>
     */
    boolean hasTaskStart();
    /**
     * <code>optional bytes taskStart = 1;</code>
     */
    com.google.protobuf.ByteString getTaskStart();

    // optional bytes taskEnd = 2;
    /**
     * <code>optional bytes taskEnd = 2;</code>
     */
    boolean hasTaskEnd();
    /**
     * <code>optional bytes taskEnd = 2;</code>
     */
    com.google.protobuf.ByteString getTaskEnd();

    // optional string className = 3;
    /**
     * <code>optional string className = 3;</code>
     */
    boolean hasClassName();
    /**
     * <code>optional string className = 3;</code>
     */
    java.lang.String getClassName();
    /**
     * <code>optional string className = 3;</code>
     */
    com.google.protobuf.ByteString
        getClassNameBytes();

    // optional bytes classBytes = 4;
    /**
     * <code>optional bytes classBytes = 4;</code>
     */
    boolean hasClassBytes();
    /**
     * <code>optional bytes classBytes = 4;</code>
     */
    com.google.protobuf.ByteString getClassBytes();

    // optional bool allowSplits = 5;
    /**
     * <code>optional bool allowSplits = 5;</code>
     */
    boolean hasAllowSplits();
    /**
     * <code>optional bool allowSplits = 5;</code>
     */
    boolean getAllowSplits();
  }
  /**
   * Protobuf type {@code SpliceSchedulerRequest}
   */
  public static final class SpliceSchedulerRequest extends
      com.google.protobuf.GeneratedMessage
      implements SpliceSchedulerRequestOrBuilder {
    // Use SpliceSchedulerRequest.newBuilder() to construct.
    private SpliceSchedulerRequest(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private SpliceSchedulerRequest(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final SpliceSchedulerRequest defaultInstance;
    public static SpliceSchedulerRequest getDefaultInstance() {
      return defaultInstance;
    }

    public SpliceSchedulerRequest getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private SpliceSchedulerRequest(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              taskStart_ = input.readBytes();
              break;
            }
            case 18: {
              bitField0_ |= 0x00000002;
              taskEnd_ = input.readBytes();
              break;
            }
            case 26: {
              bitField0_ |= 0x00000004;
              className_ = input.readBytes();
              break;
            }
            case 34: {
              bitField0_ |= 0x00000008;
              classBytes_ = input.readBytes();
              break;
            }
            case 40: {
              bitField0_ |= 0x00000010;
              allowSplits_ = input.readBool();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_SpliceSchedulerRequest_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_SpliceSchedulerRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest.class, com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest.Builder.class);
    }

    public static com.google.protobuf.Parser<SpliceSchedulerRequest> PARSER =
        new com.google.protobuf.AbstractParser<SpliceSchedulerRequest>() {
      public SpliceSchedulerRequest parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new SpliceSchedulerRequest(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<SpliceSchedulerRequest> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // optional bytes taskStart = 1;
    public static final int TASKSTART_FIELD_NUMBER = 1;
    private com.google.protobuf.ByteString taskStart_;
    /**
     * <code>optional bytes taskStart = 1;</code>
     */
    public boolean hasTaskStart() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional bytes taskStart = 1;</code>
     */
    public com.google.protobuf.ByteString getTaskStart() {
      return taskStart_;
    }

    // optional bytes taskEnd = 2;
    public static final int TASKEND_FIELD_NUMBER = 2;
    private com.google.protobuf.ByteString taskEnd_;
    /**
     * <code>optional bytes taskEnd = 2;</code>
     */
    public boolean hasTaskEnd() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional bytes taskEnd = 2;</code>
     */
    public com.google.protobuf.ByteString getTaskEnd() {
      return taskEnd_;
    }

    // optional string className = 3;
    public static final int CLASSNAME_FIELD_NUMBER = 3;
    private java.lang.Object className_;
    /**
     * <code>optional string className = 3;</code>
     */
    public boolean hasClassName() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional string className = 3;</code>
     */
    public java.lang.String getClassName() {
      java.lang.Object ref = className_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          className_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string className = 3;</code>
     */
    public com.google.protobuf.ByteString
        getClassNameBytes() {
      java.lang.Object ref = className_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        className_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // optional bytes classBytes = 4;
    public static final int CLASSBYTES_FIELD_NUMBER = 4;
    private com.google.protobuf.ByteString classBytes_;
    /**
     * <code>optional bytes classBytes = 4;</code>
     */
    public boolean hasClassBytes() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>optional bytes classBytes = 4;</code>
     */
    public com.google.protobuf.ByteString getClassBytes() {
      return classBytes_;
    }

    // optional bool allowSplits = 5;
    public static final int ALLOWSPLITS_FIELD_NUMBER = 5;
    private boolean allowSplits_;
    /**
     * <code>optional bool allowSplits = 5;</code>
     */
    public boolean hasAllowSplits() {
      return ((bitField0_ & 0x00000010) == 0x00000010);
    }
    /**
     * <code>optional bool allowSplits = 5;</code>
     */
    public boolean getAllowSplits() {
      return allowSplits_;
    }

    private void initFields() {
      taskStart_ = com.google.protobuf.ByteString.EMPTY;
      taskEnd_ = com.google.protobuf.ByteString.EMPTY;
      className_ = "";
      classBytes_ = com.google.protobuf.ByteString.EMPTY;
      allowSplits_ = false;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, taskStart_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeBytes(2, taskEnd_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeBytes(3, getClassNameBytes());
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        output.writeBytes(4, classBytes_);
      }
      if (((bitField0_ & 0x00000010) == 0x00000010)) {
        output.writeBool(5, allowSplits_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, taskStart_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(2, taskEnd_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(3, getClassNameBytes());
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(4, classBytes_);
      }
      if (((bitField0_ & 0x00000010) == 0x00000010)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBoolSize(5, allowSplits_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest)) {
        return super.equals(obj);
      }
      com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest other = (com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest) obj;

      boolean result = true;
      result = result && (hasTaskStart() == other.hasTaskStart());
      if (hasTaskStart()) {
        result = result && getTaskStart()
            .equals(other.getTaskStart());
      }
      result = result && (hasTaskEnd() == other.hasTaskEnd());
      if (hasTaskEnd()) {
        result = result && getTaskEnd()
            .equals(other.getTaskEnd());
      }
      result = result && (hasClassName() == other.hasClassName());
      if (hasClassName()) {
        result = result && getClassName()
            .equals(other.getClassName());
      }
      result = result && (hasClassBytes() == other.hasClassBytes());
      if (hasClassBytes()) {
        result = result && getClassBytes()
            .equals(other.getClassBytes());
      }
      result = result && (hasAllowSplits() == other.hasAllowSplits());
      if (hasAllowSplits()) {
        result = result && (getAllowSplits()
            == other.getAllowSplits());
      }
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }

    private int memoizedHashCode = 0;
    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      if (hasTaskStart()) {
        hash = (37 * hash) + TASKSTART_FIELD_NUMBER;
        hash = (53 * hash) + getTaskStart().hashCode();
      }
      if (hasTaskEnd()) {
        hash = (37 * hash) + TASKEND_FIELD_NUMBER;
        hash = (53 * hash) + getTaskEnd().hashCode();
      }
      if (hasClassName()) {
        hash = (37 * hash) + CLASSNAME_FIELD_NUMBER;
        hash = (53 * hash) + getClassName().hashCode();
      }
      if (hasClassBytes()) {
        hash = (37 * hash) + CLASSBYTES_FIELD_NUMBER;
        hash = (53 * hash) + getClassBytes().hashCode();
      }
      if (hasAllowSplits()) {
        hash = (37 * hash) + ALLOWSPLITS_FIELD_NUMBER;
        hash = (53 * hash) + hashBoolean(getAllowSplits());
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code SpliceSchedulerRequest}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_SpliceSchedulerRequest_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_SpliceSchedulerRequest_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest.class, com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest.Builder.class);
      }

      // Construct using com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        taskStart_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000001);
        taskEnd_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000002);
        className_ = "";
        bitField0_ = (bitField0_ & ~0x00000004);
        classBytes_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000008);
        allowSplits_ = false;
        bitField0_ = (bitField0_ & ~0x00000010);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_SpliceSchedulerRequest_descriptor;
      }

      public com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest getDefaultInstanceForType() {
        return com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest.getDefaultInstance();
      }

      public com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest build() {
        com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest buildPartial() {
        com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest result = new com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.taskStart_ = taskStart_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.taskEnd_ = taskEnd_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.className_ = className_;
        if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
          to_bitField0_ |= 0x00000008;
        }
        result.classBytes_ = classBytes_;
        if (((from_bitField0_ & 0x00000010) == 0x00000010)) {
          to_bitField0_ |= 0x00000010;
        }
        result.allowSplits_ = allowSplits_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest) {
          return mergeFrom((com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest other) {
        if (other == com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest.getDefaultInstance()) return this;
        if (other.hasTaskStart()) {
          setTaskStart(other.getTaskStart());
        }
        if (other.hasTaskEnd()) {
          setTaskEnd(other.getTaskEnd());
        }
        if (other.hasClassName()) {
          bitField0_ |= 0x00000004;
          className_ = other.className_;
          onChanged();
        }
        if (other.hasClassBytes()) {
          setClassBytes(other.getClassBytes());
        }
        if (other.hasAllowSplits()) {
          setAllowSplits(other.getAllowSplits());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // optional bytes taskStart = 1;
      private com.google.protobuf.ByteString taskStart_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes taskStart = 1;</code>
       */
      public boolean hasTaskStart() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional bytes taskStart = 1;</code>
       */
      public com.google.protobuf.ByteString getTaskStart() {
        return taskStart_;
      }
      /**
       * <code>optional bytes taskStart = 1;</code>
       */
      public Builder setTaskStart(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        taskStart_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes taskStart = 1;</code>
       */
      public Builder clearTaskStart() {
        bitField0_ = (bitField0_ & ~0x00000001);
        taskStart_ = getDefaultInstance().getTaskStart();
        onChanged();
        return this;
      }

      // optional bytes taskEnd = 2;
      private com.google.protobuf.ByteString taskEnd_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes taskEnd = 2;</code>
       */
      public boolean hasTaskEnd() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional bytes taskEnd = 2;</code>
       */
      public com.google.protobuf.ByteString getTaskEnd() {
        return taskEnd_;
      }
      /**
       * <code>optional bytes taskEnd = 2;</code>
       */
      public Builder setTaskEnd(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        taskEnd_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes taskEnd = 2;</code>
       */
      public Builder clearTaskEnd() {
        bitField0_ = (bitField0_ & ~0x00000002);
        taskEnd_ = getDefaultInstance().getTaskEnd();
        onChanged();
        return this;
      }

      // optional string className = 3;
      private java.lang.Object className_ = "";
      /**
       * <code>optional string className = 3;</code>
       */
      public boolean hasClassName() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>optional string className = 3;</code>
       */
      public java.lang.String getClassName() {
        java.lang.Object ref = className_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          className_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string className = 3;</code>
       */
      public com.google.protobuf.ByteString
          getClassNameBytes() {
        java.lang.Object ref = className_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          className_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string className = 3;</code>
       */
      public Builder setClassName(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        className_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string className = 3;</code>
       */
      public Builder clearClassName() {
        bitField0_ = (bitField0_ & ~0x00000004);
        className_ = getDefaultInstance().getClassName();
        onChanged();
        return this;
      }
      /**
       * <code>optional string className = 3;</code>
       */
      public Builder setClassNameBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        className_ = value;
        onChanged();
        return this;
      }

      // optional bytes classBytes = 4;
      private com.google.protobuf.ByteString classBytes_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes classBytes = 4;</code>
       */
      public boolean hasClassBytes() {
        return ((bitField0_ & 0x00000008) == 0x00000008);
      }
      /**
       * <code>optional bytes classBytes = 4;</code>
       */
      public com.google.protobuf.ByteString getClassBytes() {
        return classBytes_;
      }
      /**
       * <code>optional bytes classBytes = 4;</code>
       */
      public Builder setClassBytes(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000008;
        classBytes_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes classBytes = 4;</code>
       */
      public Builder clearClassBytes() {
        bitField0_ = (bitField0_ & ~0x00000008);
        classBytes_ = getDefaultInstance().getClassBytes();
        onChanged();
        return this;
      }

      // optional bool allowSplits = 5;
      private boolean allowSplits_ ;
      /**
       * <code>optional bool allowSplits = 5;</code>
       */
      public boolean hasAllowSplits() {
        return ((bitField0_ & 0x00000010) == 0x00000010);
      }
      /**
       * <code>optional bool allowSplits = 5;</code>
       */
      public boolean getAllowSplits() {
        return allowSplits_;
      }
      /**
       * <code>optional bool allowSplits = 5;</code>
       */
      public Builder setAllowSplits(boolean value) {
        bitField0_ |= 0x00000010;
        allowSplits_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bool allowSplits = 5;</code>
       */
      public Builder clearAllowSplits() {
        bitField0_ = (bitField0_ & ~0x00000010);
        allowSplits_ = false;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:SpliceSchedulerRequest)
    }

    static {
      defaultInstance = new SpliceSchedulerRequest(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:SpliceSchedulerRequest)
  }

  public interface TaskFutureResponseOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required string taskNode = 1;
    /**
     * <code>required string taskNode = 1;</code>
     */
    boolean hasTaskNode();
    /**
     * <code>required string taskNode = 1;</code>
     */
    java.lang.String getTaskNode();
    /**
     * <code>required string taskNode = 1;</code>
     */
    com.google.protobuf.ByteString
        getTaskNodeBytes();

    // required bytes taskId = 2;
    /**
     * <code>required bytes taskId = 2;</code>
     */
    boolean hasTaskId();
    /**
     * <code>required bytes taskId = 2;</code>
     */
    com.google.protobuf.ByteString getTaskId();

    // optional double estimatedCost = 3;
    /**
     * <code>optional double estimatedCost = 3;</code>
     */
    boolean hasEstimatedCost();
    /**
     * <code>optional double estimatedCost = 3;</code>
     */
    double getEstimatedCost();

    // required bytes startRow = 4;
    /**
     * <code>required bytes startRow = 4;</code>
     */
    boolean hasStartRow();
    /**
     * <code>required bytes startRow = 4;</code>
     */
    com.google.protobuf.ByteString getStartRow();
  }
  /**
   * Protobuf type {@code TaskFutureResponse}
   */
  public static final class TaskFutureResponse extends
      com.google.protobuf.GeneratedMessage
      implements TaskFutureResponseOrBuilder {
    // Use TaskFutureResponse.newBuilder() to construct.
    private TaskFutureResponse(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private TaskFutureResponse(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final TaskFutureResponse defaultInstance;
    public static TaskFutureResponse getDefaultInstance() {
      return defaultInstance;
    }

    public TaskFutureResponse getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private TaskFutureResponse(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              taskNode_ = input.readBytes();
              break;
            }
            case 18: {
              bitField0_ |= 0x00000002;
              taskId_ = input.readBytes();
              break;
            }
            case 25: {
              bitField0_ |= 0x00000004;
              estimatedCost_ = input.readDouble();
              break;
            }
            case 34: {
              bitField0_ |= 0x00000008;
              startRow_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_TaskFutureResponse_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_TaskFutureResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.class, com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.Builder.class);
    }

    public static com.google.protobuf.Parser<TaskFutureResponse> PARSER =
        new com.google.protobuf.AbstractParser<TaskFutureResponse>() {
      public TaskFutureResponse parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new TaskFutureResponse(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<TaskFutureResponse> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required string taskNode = 1;
    public static final int TASKNODE_FIELD_NUMBER = 1;
    private java.lang.Object taskNode_;
    /**
     * <code>required string taskNode = 1;</code>
     */
    public boolean hasTaskNode() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required string taskNode = 1;</code>
     */
    public java.lang.String getTaskNode() {
      java.lang.Object ref = taskNode_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          taskNode_ = s;
        }
        return s;
      }
    }
    /**
     * <code>required string taskNode = 1;</code>
     */
    public com.google.protobuf.ByteString
        getTaskNodeBytes() {
      java.lang.Object ref = taskNode_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        taskNode_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // required bytes taskId = 2;
    public static final int TASKID_FIELD_NUMBER = 2;
    private com.google.protobuf.ByteString taskId_;
    /**
     * <code>required bytes taskId = 2;</code>
     */
    public boolean hasTaskId() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>required bytes taskId = 2;</code>
     */
    public com.google.protobuf.ByteString getTaskId() {
      return taskId_;
    }

    // optional double estimatedCost = 3;
    public static final int ESTIMATEDCOST_FIELD_NUMBER = 3;
    private double estimatedCost_;
    /**
     * <code>optional double estimatedCost = 3;</code>
     */
    public boolean hasEstimatedCost() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional double estimatedCost = 3;</code>
     */
    public double getEstimatedCost() {
      return estimatedCost_;
    }

    // required bytes startRow = 4;
    public static final int STARTROW_FIELD_NUMBER = 4;
    private com.google.protobuf.ByteString startRow_;
    /**
     * <code>required bytes startRow = 4;</code>
     */
    public boolean hasStartRow() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>required bytes startRow = 4;</code>
     */
    public com.google.protobuf.ByteString getStartRow() {
      return startRow_;
    }

    private void initFields() {
      taskNode_ = "";
      taskId_ = com.google.protobuf.ByteString.EMPTY;
      estimatedCost_ = 0D;
      startRow_ = com.google.protobuf.ByteString.EMPTY;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasTaskNode()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasTaskId()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasStartRow()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, getTaskNodeBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeBytes(2, taskId_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeDouble(3, estimatedCost_);
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        output.writeBytes(4, startRow_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, getTaskNodeBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(2, taskId_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeDoubleSize(3, estimatedCost_);
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(4, startRow_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse)) {
        return super.equals(obj);
      }
      com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse other = (com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse) obj;

      boolean result = true;
      result = result && (hasTaskNode() == other.hasTaskNode());
      if (hasTaskNode()) {
        result = result && getTaskNode()
            .equals(other.getTaskNode());
      }
      result = result && (hasTaskId() == other.hasTaskId());
      if (hasTaskId()) {
        result = result && getTaskId()
            .equals(other.getTaskId());
      }
      result = result && (hasEstimatedCost() == other.hasEstimatedCost());
      if (hasEstimatedCost()) {
        result = result && (Double.doubleToLongBits(getEstimatedCost())    == Double.doubleToLongBits(other.getEstimatedCost()));
      }
      result = result && (hasStartRow() == other.hasStartRow());
      if (hasStartRow()) {
        result = result && getStartRow()
            .equals(other.getStartRow());
      }
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }

    private int memoizedHashCode = 0;
    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      if (hasTaskNode()) {
        hash = (37 * hash) + TASKNODE_FIELD_NUMBER;
        hash = (53 * hash) + getTaskNode().hashCode();
      }
      if (hasTaskId()) {
        hash = (37 * hash) + TASKID_FIELD_NUMBER;
        hash = (53 * hash) + getTaskId().hashCode();
      }
      if (hasEstimatedCost()) {
        hash = (37 * hash) + ESTIMATEDCOST_FIELD_NUMBER;
        hash = (53 * hash) + hashLong(
            Double.doubleToLongBits(getEstimatedCost()));
      }
      if (hasStartRow()) {
        hash = (37 * hash) + STARTROW_FIELD_NUMBER;
        hash = (53 * hash) + getStartRow().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code TaskFutureResponse}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_TaskFutureResponse_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_TaskFutureResponse_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.class, com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.Builder.class);
      }

      // Construct using com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        taskNode_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        taskId_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000002);
        estimatedCost_ = 0D;
        bitField0_ = (bitField0_ & ~0x00000004);
        startRow_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000008);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_TaskFutureResponse_descriptor;
      }

      public com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse getDefaultInstanceForType() {
        return com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.getDefaultInstance();
      }

      public com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse build() {
        com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse buildPartial() {
        com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse result = new com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.taskNode_ = taskNode_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.taskId_ = taskId_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.estimatedCost_ = estimatedCost_;
        if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
          to_bitField0_ |= 0x00000008;
        }
        result.startRow_ = startRow_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse) {
          return mergeFrom((com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse other) {
        if (other == com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.getDefaultInstance()) return this;
        if (other.hasTaskNode()) {
          bitField0_ |= 0x00000001;
          taskNode_ = other.taskNode_;
          onChanged();
        }
        if (other.hasTaskId()) {
          setTaskId(other.getTaskId());
        }
        if (other.hasEstimatedCost()) {
          setEstimatedCost(other.getEstimatedCost());
        }
        if (other.hasStartRow()) {
          setStartRow(other.getStartRow());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasTaskNode()) {
          
          return false;
        }
        if (!hasTaskId()) {
          
          return false;
        }
        if (!hasStartRow()) {
          
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required string taskNode = 1;
      private java.lang.Object taskNode_ = "";
      /**
       * <code>required string taskNode = 1;</code>
       */
      public boolean hasTaskNode() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required string taskNode = 1;</code>
       */
      public java.lang.String getTaskNode() {
        java.lang.Object ref = taskNode_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          taskNode_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>required string taskNode = 1;</code>
       */
      public com.google.protobuf.ByteString
          getTaskNodeBytes() {
        java.lang.Object ref = taskNode_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          taskNode_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>required string taskNode = 1;</code>
       */
      public Builder setTaskNode(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        taskNode_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required string taskNode = 1;</code>
       */
      public Builder clearTaskNode() {
        bitField0_ = (bitField0_ & ~0x00000001);
        taskNode_ = getDefaultInstance().getTaskNode();
        onChanged();
        return this;
      }
      /**
       * <code>required string taskNode = 1;</code>
       */
      public Builder setTaskNodeBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        taskNode_ = value;
        onChanged();
        return this;
      }

      // required bytes taskId = 2;
      private com.google.protobuf.ByteString taskId_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>required bytes taskId = 2;</code>
       */
      public boolean hasTaskId() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>required bytes taskId = 2;</code>
       */
      public com.google.protobuf.ByteString getTaskId() {
        return taskId_;
      }
      /**
       * <code>required bytes taskId = 2;</code>
       */
      public Builder setTaskId(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        taskId_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required bytes taskId = 2;</code>
       */
      public Builder clearTaskId() {
        bitField0_ = (bitField0_ & ~0x00000002);
        taskId_ = getDefaultInstance().getTaskId();
        onChanged();
        return this;
      }

      // optional double estimatedCost = 3;
      private double estimatedCost_ ;
      /**
       * <code>optional double estimatedCost = 3;</code>
       */
      public boolean hasEstimatedCost() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>optional double estimatedCost = 3;</code>
       */
      public double getEstimatedCost() {
        return estimatedCost_;
      }
      /**
       * <code>optional double estimatedCost = 3;</code>
       */
      public Builder setEstimatedCost(double value) {
        bitField0_ |= 0x00000004;
        estimatedCost_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional double estimatedCost = 3;</code>
       */
      public Builder clearEstimatedCost() {
        bitField0_ = (bitField0_ & ~0x00000004);
        estimatedCost_ = 0D;
        onChanged();
        return this;
      }

      // required bytes startRow = 4;
      private com.google.protobuf.ByteString startRow_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>required bytes startRow = 4;</code>
       */
      public boolean hasStartRow() {
        return ((bitField0_ & 0x00000008) == 0x00000008);
      }
      /**
       * <code>required bytes startRow = 4;</code>
       */
      public com.google.protobuf.ByteString getStartRow() {
        return startRow_;
      }
      /**
       * <code>required bytes startRow = 4;</code>
       */
      public Builder setStartRow(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000008;
        startRow_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required bytes startRow = 4;</code>
       */
      public Builder clearStartRow() {
        bitField0_ = (bitField0_ & ~0x00000008);
        startRow_ = getDefaultInstance().getStartRow();
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:TaskFutureResponse)
    }

    static {
      defaultInstance = new TaskFutureResponse(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:TaskFutureResponse)
  }

  public interface SchedulerResponseOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // repeated .TaskFutureResponse response = 1;
    /**
     * <code>repeated .TaskFutureResponse response = 1;</code>
     */
    java.util.List<com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse> 
        getResponseList();
    /**
     * <code>repeated .TaskFutureResponse response = 1;</code>
     */
    com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse getResponse(int index);
    /**
     * <code>repeated .TaskFutureResponse response = 1;</code>
     */
    int getResponseCount();
    /**
     * <code>repeated .TaskFutureResponse response = 1;</code>
     */
    java.util.List<? extends com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponseOrBuilder> 
        getResponseOrBuilderList();
    /**
     * <code>repeated .TaskFutureResponse response = 1;</code>
     */
    com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponseOrBuilder getResponseOrBuilder(
        int index);
  }
  /**
   * Protobuf type {@code SchedulerResponse}
   */
  public static final class SchedulerResponse extends
      com.google.protobuf.GeneratedMessage
      implements SchedulerResponseOrBuilder {
    // Use SchedulerResponse.newBuilder() to construct.
    private SchedulerResponse(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private SchedulerResponse(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final SchedulerResponse defaultInstance;
    public static SchedulerResponse getDefaultInstance() {
      return defaultInstance;
    }

    public SchedulerResponse getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private SchedulerResponse(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              if (!((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
                response_ = new java.util.ArrayList<com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse>();
                mutable_bitField0_ |= 0x00000001;
              }
              response_.add(input.readMessage(com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.PARSER, extensionRegistry));
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        if (((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
          response_ = java.util.Collections.unmodifiableList(response_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_SchedulerResponse_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_SchedulerResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse.class, com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse.Builder.class);
    }

    public static com.google.protobuf.Parser<SchedulerResponse> PARSER =
        new com.google.protobuf.AbstractParser<SchedulerResponse>() {
      public SchedulerResponse parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new SchedulerResponse(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<SchedulerResponse> getParserForType() {
      return PARSER;
    }

    // repeated .TaskFutureResponse response = 1;
    public static final int RESPONSE_FIELD_NUMBER = 1;
    private java.util.List<com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse> response_;
    /**
     * <code>repeated .TaskFutureResponse response = 1;</code>
     */
    public java.util.List<com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse> getResponseList() {
      return response_;
    }
    /**
     * <code>repeated .TaskFutureResponse response = 1;</code>
     */
    public java.util.List<? extends com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponseOrBuilder> 
        getResponseOrBuilderList() {
      return response_;
    }
    /**
     * <code>repeated .TaskFutureResponse response = 1;</code>
     */
    public int getResponseCount() {
      return response_.size();
    }
    /**
     * <code>repeated .TaskFutureResponse response = 1;</code>
     */
    public com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse getResponse(int index) {
      return response_.get(index);
    }
    /**
     * <code>repeated .TaskFutureResponse response = 1;</code>
     */
    public com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponseOrBuilder getResponseOrBuilder(
        int index) {
      return response_.get(index);
    }

    private void initFields() {
      response_ = java.util.Collections.emptyList();
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      for (int i = 0; i < getResponseCount(); i++) {
        if (!getResponse(i).isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      for (int i = 0; i < response_.size(); i++) {
        output.writeMessage(1, response_.get(i));
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      for (int i = 0; i < response_.size(); i++) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(1, response_.get(i));
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse)) {
        return super.equals(obj);
      }
      com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse other = (com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse) obj;

      boolean result = true;
      result = result && getResponseList()
          .equals(other.getResponseList());
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }

    private int memoizedHashCode = 0;
    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      if (getResponseCount() > 0) {
        hash = (37 * hash) + RESPONSE_FIELD_NUMBER;
        hash = (53 * hash) + getResponseList().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code SchedulerResponse}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.splicemachine.coprocessor.SpliceMessage.SchedulerResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_SchedulerResponse_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_SchedulerResponse_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse.class, com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse.Builder.class);
      }

      // Construct using com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
          getResponseFieldBuilder();
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        if (responseBuilder_ == null) {
          response_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000001);
        } else {
          responseBuilder_.clear();
        }
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_SchedulerResponse_descriptor;
      }

      public com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse getDefaultInstanceForType() {
        return com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse.getDefaultInstance();
      }

      public com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse build() {
        com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse buildPartial() {
        com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse result = new com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse(this);
        int from_bitField0_ = bitField0_;
        if (responseBuilder_ == null) {
          if (((bitField0_ & 0x00000001) == 0x00000001)) {
            response_ = java.util.Collections.unmodifiableList(response_);
            bitField0_ = (bitField0_ & ~0x00000001);
          }
          result.response_ = response_;
        } else {
          result.response_ = responseBuilder_.build();
        }
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse) {
          return mergeFrom((com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse other) {
        if (other == com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse.getDefaultInstance()) return this;
        if (responseBuilder_ == null) {
          if (!other.response_.isEmpty()) {
            if (response_.isEmpty()) {
              response_ = other.response_;
              bitField0_ = (bitField0_ & ~0x00000001);
            } else {
              ensureResponseIsMutable();
              response_.addAll(other.response_);
            }
            onChanged();
          }
        } else {
          if (!other.response_.isEmpty()) {
            if (responseBuilder_.isEmpty()) {
              responseBuilder_.dispose();
              responseBuilder_ = null;
              response_ = other.response_;
              bitField0_ = (bitField0_ & ~0x00000001);
              responseBuilder_ = 
                com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders ?
                   getResponseFieldBuilder() : null;
            } else {
              responseBuilder_.addAllMessages(other.response_);
            }
          }
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        for (int i = 0; i < getResponseCount(); i++) {
          if (!getResponse(i).isInitialized()) {
            
            return false;
          }
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // repeated .TaskFutureResponse response = 1;
      private java.util.List<com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse> response_ =
        java.util.Collections.emptyList();
      private void ensureResponseIsMutable() {
        if (!((bitField0_ & 0x00000001) == 0x00000001)) {
          response_ = new java.util.ArrayList<com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse>(response_);
          bitField0_ |= 0x00000001;
         }
      }

      private com.google.protobuf.RepeatedFieldBuilder<
          com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse, com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.Builder, com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponseOrBuilder> responseBuilder_;

      /**
       * <code>repeated .TaskFutureResponse response = 1;</code>
       */
      public java.util.List<com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse> getResponseList() {
        if (responseBuilder_ == null) {
          return java.util.Collections.unmodifiableList(response_);
        } else {
          return responseBuilder_.getMessageList();
        }
      }
      /**
       * <code>repeated .TaskFutureResponse response = 1;</code>
       */
      public int getResponseCount() {
        if (responseBuilder_ == null) {
          return response_.size();
        } else {
          return responseBuilder_.getCount();
        }
      }
      /**
       * <code>repeated .TaskFutureResponse response = 1;</code>
       */
      public com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse getResponse(int index) {
        if (responseBuilder_ == null) {
          return response_.get(index);
        } else {
          return responseBuilder_.getMessage(index);
        }
      }
      /**
       * <code>repeated .TaskFutureResponse response = 1;</code>
       */
      public Builder setResponse(
          int index, com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse value) {
        if (responseBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureResponseIsMutable();
          response_.set(index, value);
          onChanged();
        } else {
          responseBuilder_.setMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .TaskFutureResponse response = 1;</code>
       */
      public Builder setResponse(
          int index, com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.Builder builderForValue) {
        if (responseBuilder_ == null) {
          ensureResponseIsMutable();
          response_.set(index, builderForValue.build());
          onChanged();
        } else {
          responseBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .TaskFutureResponse response = 1;</code>
       */
      public Builder addResponse(com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse value) {
        if (responseBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureResponseIsMutable();
          response_.add(value);
          onChanged();
        } else {
          responseBuilder_.addMessage(value);
        }
        return this;
      }
      /**
       * <code>repeated .TaskFutureResponse response = 1;</code>
       */
      public Builder addResponse(
          int index, com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse value) {
        if (responseBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureResponseIsMutable();
          response_.add(index, value);
          onChanged();
        } else {
          responseBuilder_.addMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .TaskFutureResponse response = 1;</code>
       */
      public Builder addResponse(
          com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.Builder builderForValue) {
        if (responseBuilder_ == null) {
          ensureResponseIsMutable();
          response_.add(builderForValue.build());
          onChanged();
        } else {
          responseBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .TaskFutureResponse response = 1;</code>
       */
      public Builder addResponse(
          int index, com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.Builder builderForValue) {
        if (responseBuilder_ == null) {
          ensureResponseIsMutable();
          response_.add(index, builderForValue.build());
          onChanged();
        } else {
          responseBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .TaskFutureResponse response = 1;</code>
       */
      public Builder addAllResponse(
          java.lang.Iterable<? extends com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse> values) {
        if (responseBuilder_ == null) {
          ensureResponseIsMutable();
          super.addAll(values, response_);
          onChanged();
        } else {
          responseBuilder_.addAllMessages(values);
        }
        return this;
      }
      /**
       * <code>repeated .TaskFutureResponse response = 1;</code>
       */
      public Builder clearResponse() {
        if (responseBuilder_ == null) {
          response_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000001);
          onChanged();
        } else {
          responseBuilder_.clear();
        }
        return this;
      }
      /**
       * <code>repeated .TaskFutureResponse response = 1;</code>
       */
      public Builder removeResponse(int index) {
        if (responseBuilder_ == null) {
          ensureResponseIsMutable();
          response_.remove(index);
          onChanged();
        } else {
          responseBuilder_.remove(index);
        }
        return this;
      }
      /**
       * <code>repeated .TaskFutureResponse response = 1;</code>
       */
      public com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.Builder getResponseBuilder(
          int index) {
        return getResponseFieldBuilder().getBuilder(index);
      }
      /**
       * <code>repeated .TaskFutureResponse response = 1;</code>
       */
      public com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponseOrBuilder getResponseOrBuilder(
          int index) {
        if (responseBuilder_ == null) {
          return response_.get(index);  } else {
          return responseBuilder_.getMessageOrBuilder(index);
        }
      }
      /**
       * <code>repeated .TaskFutureResponse response = 1;</code>
       */
      public java.util.List<? extends com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponseOrBuilder> 
           getResponseOrBuilderList() {
        if (responseBuilder_ != null) {
          return responseBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(response_);
        }
      }
      /**
       * <code>repeated .TaskFutureResponse response = 1;</code>
       */
      public com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.Builder addResponseBuilder() {
        return getResponseFieldBuilder().addBuilder(
            com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.getDefaultInstance());
      }
      /**
       * <code>repeated .TaskFutureResponse response = 1;</code>
       */
      public com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.Builder addResponseBuilder(
          int index) {
        return getResponseFieldBuilder().addBuilder(
            index, com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.getDefaultInstance());
      }
      /**
       * <code>repeated .TaskFutureResponse response = 1;</code>
       */
      public java.util.List<com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.Builder> 
           getResponseBuilderList() {
        return getResponseFieldBuilder().getBuilderList();
      }
      private com.google.protobuf.RepeatedFieldBuilder<
          com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse, com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.Builder, com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponseOrBuilder> 
          getResponseFieldBuilder() {
        if (responseBuilder_ == null) {
          responseBuilder_ = new com.google.protobuf.RepeatedFieldBuilder<
              com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse, com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponse.Builder, com.splicemachine.coprocessor.SpliceMessage.TaskFutureResponseOrBuilder>(
                  response_,
                  ((bitField0_ & 0x00000001) == 0x00000001),
                  getParentForChildren(),
                  isClean());
          response_ = null;
        }
        return responseBuilder_;
      }

      // @@protoc_insertion_point(builder_scope:SchedulerResponse)
    }

    static {
      defaultInstance = new SchedulerResponse(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:SchedulerResponse)
  }

  public interface DeleteFirstAfterRequestOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // optional string transactionId = 1;
    /**
     * <code>optional string transactionId = 1;</code>
     */
    boolean hasTransactionId();
    /**
     * <code>optional string transactionId = 1;</code>
     */
    java.lang.String getTransactionId();
    /**
     * <code>optional string transactionId = 1;</code>
     */
    com.google.protobuf.ByteString
        getTransactionIdBytes();

    // optional bytes rowKey = 2;
    /**
     * <code>optional bytes rowKey = 2;</code>
     */
    boolean hasRowKey();
    /**
     * <code>optional bytes rowKey = 2;</code>
     */
    com.google.protobuf.ByteString getRowKey();

    // optional bytes limit = 3;
    /**
     * <code>optional bytes limit = 3;</code>
     */
    boolean hasLimit();
    /**
     * <code>optional bytes limit = 3;</code>
     */
    com.google.protobuf.ByteString getLimit();
  }
  /**
   * Protobuf type {@code DeleteFirstAfterRequest}
   */
  public static final class DeleteFirstAfterRequest extends
      com.google.protobuf.GeneratedMessage
      implements DeleteFirstAfterRequestOrBuilder {
    // Use DeleteFirstAfterRequest.newBuilder() to construct.
    private DeleteFirstAfterRequest(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private DeleteFirstAfterRequest(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final DeleteFirstAfterRequest defaultInstance;
    public static DeleteFirstAfterRequest getDefaultInstance() {
      return defaultInstance;
    }

    public DeleteFirstAfterRequest getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private DeleteFirstAfterRequest(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              transactionId_ = input.readBytes();
              break;
            }
            case 18: {
              bitField0_ |= 0x00000002;
              rowKey_ = input.readBytes();
              break;
            }
            case 26: {
              bitField0_ |= 0x00000004;
              limit_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_DeleteFirstAfterRequest_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_DeleteFirstAfterRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest.class, com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest.Builder.class);
    }

    public static com.google.protobuf.Parser<DeleteFirstAfterRequest> PARSER =
        new com.google.protobuf.AbstractParser<DeleteFirstAfterRequest>() {
      public DeleteFirstAfterRequest parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new DeleteFirstAfterRequest(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<DeleteFirstAfterRequest> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // optional string transactionId = 1;
    public static final int TRANSACTIONID_FIELD_NUMBER = 1;
    private java.lang.Object transactionId_;
    /**
     * <code>optional string transactionId = 1;</code>
     */
    public boolean hasTransactionId() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional string transactionId = 1;</code>
     */
    public java.lang.String getTransactionId() {
      java.lang.Object ref = transactionId_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          transactionId_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string transactionId = 1;</code>
     */
    public com.google.protobuf.ByteString
        getTransactionIdBytes() {
      java.lang.Object ref = transactionId_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        transactionId_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // optional bytes rowKey = 2;
    public static final int ROWKEY_FIELD_NUMBER = 2;
    private com.google.protobuf.ByteString rowKey_;
    /**
     * <code>optional bytes rowKey = 2;</code>
     */
    public boolean hasRowKey() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional bytes rowKey = 2;</code>
     */
    public com.google.protobuf.ByteString getRowKey() {
      return rowKey_;
    }

    // optional bytes limit = 3;
    public static final int LIMIT_FIELD_NUMBER = 3;
    private com.google.protobuf.ByteString limit_;
    /**
     * <code>optional bytes limit = 3;</code>
     */
    public boolean hasLimit() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional bytes limit = 3;</code>
     */
    public com.google.protobuf.ByteString getLimit() {
      return limit_;
    }

    private void initFields() {
      transactionId_ = "";
      rowKey_ = com.google.protobuf.ByteString.EMPTY;
      limit_ = com.google.protobuf.ByteString.EMPTY;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, getTransactionIdBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeBytes(2, rowKey_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeBytes(3, limit_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, getTransactionIdBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(2, rowKey_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(3, limit_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest)) {
        return super.equals(obj);
      }
      com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest other = (com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest) obj;

      boolean result = true;
      result = result && (hasTransactionId() == other.hasTransactionId());
      if (hasTransactionId()) {
        result = result && getTransactionId()
            .equals(other.getTransactionId());
      }
      result = result && (hasRowKey() == other.hasRowKey());
      if (hasRowKey()) {
        result = result && getRowKey()
            .equals(other.getRowKey());
      }
      result = result && (hasLimit() == other.hasLimit());
      if (hasLimit()) {
        result = result && getLimit()
            .equals(other.getLimit());
      }
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }

    private int memoizedHashCode = 0;
    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      if (hasTransactionId()) {
        hash = (37 * hash) + TRANSACTIONID_FIELD_NUMBER;
        hash = (53 * hash) + getTransactionId().hashCode();
      }
      if (hasRowKey()) {
        hash = (37 * hash) + ROWKEY_FIELD_NUMBER;
        hash = (53 * hash) + getRowKey().hashCode();
      }
      if (hasLimit()) {
        hash = (37 * hash) + LIMIT_FIELD_NUMBER;
        hash = (53 * hash) + getLimit().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code DeleteFirstAfterRequest}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_DeleteFirstAfterRequest_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_DeleteFirstAfterRequest_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest.class, com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest.Builder.class);
      }

      // Construct using com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        transactionId_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        rowKey_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000002);
        limit_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_DeleteFirstAfterRequest_descriptor;
      }

      public com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest getDefaultInstanceForType() {
        return com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest.getDefaultInstance();
      }

      public com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest build() {
        com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest buildPartial() {
        com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest result = new com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.transactionId_ = transactionId_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.rowKey_ = rowKey_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.limit_ = limit_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest) {
          return mergeFrom((com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest other) {
        if (other == com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest.getDefaultInstance()) return this;
        if (other.hasTransactionId()) {
          bitField0_ |= 0x00000001;
          transactionId_ = other.transactionId_;
          onChanged();
        }
        if (other.hasRowKey()) {
          setRowKey(other.getRowKey());
        }
        if (other.hasLimit()) {
          setLimit(other.getLimit());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // optional string transactionId = 1;
      private java.lang.Object transactionId_ = "";
      /**
       * <code>optional string transactionId = 1;</code>
       */
      public boolean hasTransactionId() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional string transactionId = 1;</code>
       */
      public java.lang.String getTransactionId() {
        java.lang.Object ref = transactionId_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          transactionId_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string transactionId = 1;</code>
       */
      public com.google.protobuf.ByteString
          getTransactionIdBytes() {
        java.lang.Object ref = transactionId_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          transactionId_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string transactionId = 1;</code>
       */
      public Builder setTransactionId(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        transactionId_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string transactionId = 1;</code>
       */
      public Builder clearTransactionId() {
        bitField0_ = (bitField0_ & ~0x00000001);
        transactionId_ = getDefaultInstance().getTransactionId();
        onChanged();
        return this;
      }
      /**
       * <code>optional string transactionId = 1;</code>
       */
      public Builder setTransactionIdBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        transactionId_ = value;
        onChanged();
        return this;
      }

      // optional bytes rowKey = 2;
      private com.google.protobuf.ByteString rowKey_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes rowKey = 2;</code>
       */
      public boolean hasRowKey() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional bytes rowKey = 2;</code>
       */
      public com.google.protobuf.ByteString getRowKey() {
        return rowKey_;
      }
      /**
       * <code>optional bytes rowKey = 2;</code>
       */
      public Builder setRowKey(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        rowKey_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes rowKey = 2;</code>
       */
      public Builder clearRowKey() {
        bitField0_ = (bitField0_ & ~0x00000002);
        rowKey_ = getDefaultInstance().getRowKey();
        onChanged();
        return this;
      }

      // optional bytes limit = 3;
      private com.google.protobuf.ByteString limit_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes limit = 3;</code>
       */
      public boolean hasLimit() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>optional bytes limit = 3;</code>
       */
      public com.google.protobuf.ByteString getLimit() {
        return limit_;
      }
      /**
       * <code>optional bytes limit = 3;</code>
       */
      public Builder setLimit(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        limit_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes limit = 3;</code>
       */
      public Builder clearLimit() {
        bitField0_ = (bitField0_ & ~0x00000004);
        limit_ = getDefaultInstance().getLimit();
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:DeleteFirstAfterRequest)
    }

    static {
      defaultInstance = new DeleteFirstAfterRequest(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:DeleteFirstAfterRequest)
  }

  public interface ConstraintContextOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // optional string tableName = 1;
    /**
     * <code>optional string tableName = 1;</code>
     */
    boolean hasTableName();
    /**
     * <code>optional string tableName = 1;</code>
     */
    java.lang.String getTableName();
    /**
     * <code>optional string tableName = 1;</code>
     */
    com.google.protobuf.ByteString
        getTableNameBytes();

    // optional string constraintName = 2;
    /**
     * <code>optional string constraintName = 2;</code>
     */
    boolean hasConstraintName();
    /**
     * <code>optional string constraintName = 2;</code>
     */
    java.lang.String getConstraintName();
    /**
     * <code>optional string constraintName = 2;</code>
     */
    com.google.protobuf.ByteString
        getConstraintNameBytes();
  }
  /**
   * Protobuf type {@code ConstraintContext}
   */
  public static final class ConstraintContext extends
      com.google.protobuf.GeneratedMessage
      implements ConstraintContextOrBuilder {
    // Use ConstraintContext.newBuilder() to construct.
    private ConstraintContext(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private ConstraintContext(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final ConstraintContext defaultInstance;
    public static ConstraintContext getDefaultInstance() {
      return defaultInstance;
    }

    public ConstraintContext getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private ConstraintContext(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              tableName_ = input.readBytes();
              break;
            }
            case 18: {
              bitField0_ |= 0x00000002;
              constraintName_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_ConstraintContext_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_ConstraintContext_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.class, com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.Builder.class);
    }

    public static com.google.protobuf.Parser<ConstraintContext> PARSER =
        new com.google.protobuf.AbstractParser<ConstraintContext>() {
      public ConstraintContext parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new ConstraintContext(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<ConstraintContext> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // optional string tableName = 1;
    public static final int TABLENAME_FIELD_NUMBER = 1;
    private java.lang.Object tableName_;
    /**
     * <code>optional string tableName = 1;</code>
     */
    public boolean hasTableName() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional string tableName = 1;</code>
     */
    public java.lang.String getTableName() {
      java.lang.Object ref = tableName_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          tableName_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string tableName = 1;</code>
     */
    public com.google.protobuf.ByteString
        getTableNameBytes() {
      java.lang.Object ref = tableName_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        tableName_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // optional string constraintName = 2;
    public static final int CONSTRAINTNAME_FIELD_NUMBER = 2;
    private java.lang.Object constraintName_;
    /**
     * <code>optional string constraintName = 2;</code>
     */
    public boolean hasConstraintName() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional string constraintName = 2;</code>
     */
    public java.lang.String getConstraintName() {
      java.lang.Object ref = constraintName_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          constraintName_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string constraintName = 2;</code>
     */
    public com.google.protobuf.ByteString
        getConstraintNameBytes() {
      java.lang.Object ref = constraintName_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        constraintName_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private void initFields() {
      tableName_ = "";
      constraintName_ = "";
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, getTableNameBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeBytes(2, getConstraintNameBytes());
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, getTableNameBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(2, getConstraintNameBytes());
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.splicemachine.coprocessor.SpliceMessage.ConstraintContext)) {
        return super.equals(obj);
      }
      com.splicemachine.coprocessor.SpliceMessage.ConstraintContext other = (com.splicemachine.coprocessor.SpliceMessage.ConstraintContext) obj;

      boolean result = true;
      result = result && (hasTableName() == other.hasTableName());
      if (hasTableName()) {
        result = result && getTableName()
            .equals(other.getTableName());
      }
      result = result && (hasConstraintName() == other.hasConstraintName());
      if (hasConstraintName()) {
        result = result && getConstraintName()
            .equals(other.getConstraintName());
      }
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }

    private int memoizedHashCode = 0;
    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      if (hasTableName()) {
        hash = (37 * hash) + TABLENAME_FIELD_NUMBER;
        hash = (53 * hash) + getTableName().hashCode();
      }
      if (hasConstraintName()) {
        hash = (37 * hash) + CONSTRAINTNAME_FIELD_NUMBER;
        hash = (53 * hash) + getConstraintName().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.splicemachine.coprocessor.SpliceMessage.ConstraintContext parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.ConstraintContext parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.ConstraintContext parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.ConstraintContext parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.ConstraintContext parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.ConstraintContext parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.ConstraintContext parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.ConstraintContext parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.ConstraintContext parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.ConstraintContext parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.splicemachine.coprocessor.SpliceMessage.ConstraintContext prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code ConstraintContext}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.splicemachine.coprocessor.SpliceMessage.ConstraintContextOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_ConstraintContext_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_ConstraintContext_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.class, com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.Builder.class);
      }

      // Construct using com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        tableName_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        constraintName_ = "";
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_ConstraintContext_descriptor;
      }

      public com.splicemachine.coprocessor.SpliceMessage.ConstraintContext getDefaultInstanceForType() {
        return com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.getDefaultInstance();
      }

      public com.splicemachine.coprocessor.SpliceMessage.ConstraintContext build() {
        com.splicemachine.coprocessor.SpliceMessage.ConstraintContext result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.splicemachine.coprocessor.SpliceMessage.ConstraintContext buildPartial() {
        com.splicemachine.coprocessor.SpliceMessage.ConstraintContext result = new com.splicemachine.coprocessor.SpliceMessage.ConstraintContext(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.tableName_ = tableName_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.constraintName_ = constraintName_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.splicemachine.coprocessor.SpliceMessage.ConstraintContext) {
          return mergeFrom((com.splicemachine.coprocessor.SpliceMessage.ConstraintContext)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.splicemachine.coprocessor.SpliceMessage.ConstraintContext other) {
        if (other == com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.getDefaultInstance()) return this;
        if (other.hasTableName()) {
          bitField0_ |= 0x00000001;
          tableName_ = other.tableName_;
          onChanged();
        }
        if (other.hasConstraintName()) {
          bitField0_ |= 0x00000002;
          constraintName_ = other.constraintName_;
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.splicemachine.coprocessor.SpliceMessage.ConstraintContext parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.splicemachine.coprocessor.SpliceMessage.ConstraintContext) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // optional string tableName = 1;
      private java.lang.Object tableName_ = "";
      /**
       * <code>optional string tableName = 1;</code>
       */
      public boolean hasTableName() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional string tableName = 1;</code>
       */
      public java.lang.String getTableName() {
        java.lang.Object ref = tableName_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          tableName_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string tableName = 1;</code>
       */
      public com.google.protobuf.ByteString
          getTableNameBytes() {
        java.lang.Object ref = tableName_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          tableName_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string tableName = 1;</code>
       */
      public Builder setTableName(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        tableName_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string tableName = 1;</code>
       */
      public Builder clearTableName() {
        bitField0_ = (bitField0_ & ~0x00000001);
        tableName_ = getDefaultInstance().getTableName();
        onChanged();
        return this;
      }
      /**
       * <code>optional string tableName = 1;</code>
       */
      public Builder setTableNameBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        tableName_ = value;
        onChanged();
        return this;
      }

      // optional string constraintName = 2;
      private java.lang.Object constraintName_ = "";
      /**
       * <code>optional string constraintName = 2;</code>
       */
      public boolean hasConstraintName() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional string constraintName = 2;</code>
       */
      public java.lang.String getConstraintName() {
        java.lang.Object ref = constraintName_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          constraintName_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string constraintName = 2;</code>
       */
      public com.google.protobuf.ByteString
          getConstraintNameBytes() {
        java.lang.Object ref = constraintName_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          constraintName_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string constraintName = 2;</code>
       */
      public Builder setConstraintName(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        constraintName_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string constraintName = 2;</code>
       */
      public Builder clearConstraintName() {
        bitField0_ = (bitField0_ & ~0x00000002);
        constraintName_ = getDefaultInstance().getConstraintName();
        onChanged();
        return this;
      }
      /**
       * <code>optional string constraintName = 2;</code>
       */
      public Builder setConstraintNameBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        constraintName_ = value;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:ConstraintContext)
    }

    static {
      defaultInstance = new ConstraintContext(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:ConstraintContext)
  }

  public interface AllocateFilterMessageOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // optional bytes addressMatch = 1;
    /**
     * <code>optional bytes addressMatch = 1;</code>
     */
    boolean hasAddressMatch();
    /**
     * <code>optional bytes addressMatch = 1;</code>
     */
    com.google.protobuf.ByteString getAddressMatch();
  }
  /**
   * Protobuf type {@code AllocateFilterMessage}
   */
  public static final class AllocateFilterMessage extends
      com.google.protobuf.GeneratedMessage
      implements AllocateFilterMessageOrBuilder {
    // Use AllocateFilterMessage.newBuilder() to construct.
    private AllocateFilterMessage(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private AllocateFilterMessage(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final AllocateFilterMessage defaultInstance;
    public static AllocateFilterMessage getDefaultInstance() {
      return defaultInstance;
    }

    public AllocateFilterMessage getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private AllocateFilterMessage(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              addressMatch_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_AllocateFilterMessage_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_AllocateFilterMessage_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage.class, com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage.Builder.class);
    }

    public static com.google.protobuf.Parser<AllocateFilterMessage> PARSER =
        new com.google.protobuf.AbstractParser<AllocateFilterMessage>() {
      public AllocateFilterMessage parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new AllocateFilterMessage(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<AllocateFilterMessage> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // optional bytes addressMatch = 1;
    public static final int ADDRESSMATCH_FIELD_NUMBER = 1;
    private com.google.protobuf.ByteString addressMatch_;
    /**
     * <code>optional bytes addressMatch = 1;</code>
     */
    public boolean hasAddressMatch() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional bytes addressMatch = 1;</code>
     */
    public com.google.protobuf.ByteString getAddressMatch() {
      return addressMatch_;
    }

    private void initFields() {
      addressMatch_ = com.google.protobuf.ByteString.EMPTY;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, addressMatch_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, addressMatch_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage)) {
        return super.equals(obj);
      }
      com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage other = (com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage) obj;

      boolean result = true;
      result = result && (hasAddressMatch() == other.hasAddressMatch());
      if (hasAddressMatch()) {
        result = result && getAddressMatch()
            .equals(other.getAddressMatch());
      }
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }

    private int memoizedHashCode = 0;
    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      if (hasAddressMatch()) {
        hash = (37 * hash) + ADDRESSMATCH_FIELD_NUMBER;
        hash = (53 * hash) + getAddressMatch().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code AllocateFilterMessage}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessageOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_AllocateFilterMessage_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_AllocateFilterMessage_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage.class, com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage.Builder.class);
      }

      // Construct using com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        addressMatch_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_AllocateFilterMessage_descriptor;
      }

      public com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage getDefaultInstanceForType() {
        return com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage.getDefaultInstance();
      }

      public com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage build() {
        com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage buildPartial() {
        com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage result = new com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.addressMatch_ = addressMatch_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage) {
          return mergeFrom((com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage other) {
        if (other == com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage.getDefaultInstance()) return this;
        if (other.hasAddressMatch()) {
          setAddressMatch(other.getAddressMatch());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.splicemachine.coprocessor.SpliceMessage.AllocateFilterMessage) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // optional bytes addressMatch = 1;
      private com.google.protobuf.ByteString addressMatch_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes addressMatch = 1;</code>
       */
      public boolean hasAddressMatch() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional bytes addressMatch = 1;</code>
       */
      public com.google.protobuf.ByteString getAddressMatch() {
        return addressMatch_;
      }
      /**
       * <code>optional bytes addressMatch = 1;</code>
       */
      public Builder setAddressMatch(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        addressMatch_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes addressMatch = 1;</code>
       */
      public Builder clearAddressMatch() {
        bitField0_ = (bitField0_ & ~0x00000001);
        addressMatch_ = getDefaultInstance().getAddressMatch();
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:AllocateFilterMessage)
    }

    static {
      defaultInstance = new AllocateFilterMessage(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:AllocateFilterMessage)
  }

  public interface SuccessFilterMessageOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // repeated bytes failedTasks = 1;
    /**
     * <code>repeated bytes failedTasks = 1;</code>
     */
    java.util.List<com.google.protobuf.ByteString> getFailedTasksList();
    /**
     * <code>repeated bytes failedTasks = 1;</code>
     */
    int getFailedTasksCount();
    /**
     * <code>repeated bytes failedTasks = 1;</code>
     */
    com.google.protobuf.ByteString getFailedTasks(int index);
  }
  /**
   * Protobuf type {@code SuccessFilterMessage}
   */
  public static final class SuccessFilterMessage extends
      com.google.protobuf.GeneratedMessage
      implements SuccessFilterMessageOrBuilder {
    // Use SuccessFilterMessage.newBuilder() to construct.
    private SuccessFilterMessage(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private SuccessFilterMessage(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final SuccessFilterMessage defaultInstance;
    public static SuccessFilterMessage getDefaultInstance() {
      return defaultInstance;
    }

    public SuccessFilterMessage getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private SuccessFilterMessage(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              if (!((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
                failedTasks_ = new java.util.ArrayList<com.google.protobuf.ByteString>();
                mutable_bitField0_ |= 0x00000001;
              }
              failedTasks_.add(input.readBytes());
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        if (((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
          failedTasks_ = java.util.Collections.unmodifiableList(failedTasks_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_SuccessFilterMessage_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_SuccessFilterMessage_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage.class, com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage.Builder.class);
    }

    public static com.google.protobuf.Parser<SuccessFilterMessage> PARSER =
        new com.google.protobuf.AbstractParser<SuccessFilterMessage>() {
      public SuccessFilterMessage parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new SuccessFilterMessage(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<SuccessFilterMessage> getParserForType() {
      return PARSER;
    }

    // repeated bytes failedTasks = 1;
    public static final int FAILEDTASKS_FIELD_NUMBER = 1;
    private java.util.List<com.google.protobuf.ByteString> failedTasks_;
    /**
     * <code>repeated bytes failedTasks = 1;</code>
     */
    public java.util.List<com.google.protobuf.ByteString>
        getFailedTasksList() {
      return failedTasks_;
    }
    /**
     * <code>repeated bytes failedTasks = 1;</code>
     */
    public int getFailedTasksCount() {
      return failedTasks_.size();
    }
    /**
     * <code>repeated bytes failedTasks = 1;</code>
     */
    public com.google.protobuf.ByteString getFailedTasks(int index) {
      return failedTasks_.get(index);
    }

    private void initFields() {
      failedTasks_ = java.util.Collections.emptyList();
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      for (int i = 0; i < failedTasks_.size(); i++) {
        output.writeBytes(1, failedTasks_.get(i));
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      {
        int dataSize = 0;
        for (int i = 0; i < failedTasks_.size(); i++) {
          dataSize += com.google.protobuf.CodedOutputStream
            .computeBytesSizeNoTag(failedTasks_.get(i));
        }
        size += dataSize;
        size += 1 * getFailedTasksList().size();
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage)) {
        return super.equals(obj);
      }
      com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage other = (com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage) obj;

      boolean result = true;
      result = result && getFailedTasksList()
          .equals(other.getFailedTasksList());
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }

    private int memoizedHashCode = 0;
    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      if (getFailedTasksCount() > 0) {
        hash = (37 * hash) + FAILEDTASKS_FIELD_NUMBER;
        hash = (53 * hash) + getFailedTasksList().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code SuccessFilterMessage}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessageOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_SuccessFilterMessage_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_SuccessFilterMessage_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage.class, com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage.Builder.class);
      }

      // Construct using com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        failedTasks_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_SuccessFilterMessage_descriptor;
      }

      public com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage getDefaultInstanceForType() {
        return com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage.getDefaultInstance();
      }

      public com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage build() {
        com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage buildPartial() {
        com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage result = new com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage(this);
        int from_bitField0_ = bitField0_;
        if (((bitField0_ & 0x00000001) == 0x00000001)) {
          failedTasks_ = java.util.Collections.unmodifiableList(failedTasks_);
          bitField0_ = (bitField0_ & ~0x00000001);
        }
        result.failedTasks_ = failedTasks_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage) {
          return mergeFrom((com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage other) {
        if (other == com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage.getDefaultInstance()) return this;
        if (!other.failedTasks_.isEmpty()) {
          if (failedTasks_.isEmpty()) {
            failedTasks_ = other.failedTasks_;
            bitField0_ = (bitField0_ & ~0x00000001);
          } else {
            ensureFailedTasksIsMutable();
            failedTasks_.addAll(other.failedTasks_);
          }
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.splicemachine.coprocessor.SpliceMessage.SuccessFilterMessage) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // repeated bytes failedTasks = 1;
      private java.util.List<com.google.protobuf.ByteString> failedTasks_ = java.util.Collections.emptyList();
      private void ensureFailedTasksIsMutable() {
        if (!((bitField0_ & 0x00000001) == 0x00000001)) {
          failedTasks_ = new java.util.ArrayList<com.google.protobuf.ByteString>(failedTasks_);
          bitField0_ |= 0x00000001;
         }
      }
      /**
       * <code>repeated bytes failedTasks = 1;</code>
       */
      public java.util.List<com.google.protobuf.ByteString>
          getFailedTasksList() {
        return java.util.Collections.unmodifiableList(failedTasks_);
      }
      /**
       * <code>repeated bytes failedTasks = 1;</code>
       */
      public int getFailedTasksCount() {
        return failedTasks_.size();
      }
      /**
       * <code>repeated bytes failedTasks = 1;</code>
       */
      public com.google.protobuf.ByteString getFailedTasks(int index) {
        return failedTasks_.get(index);
      }
      /**
       * <code>repeated bytes failedTasks = 1;</code>
       */
      public Builder setFailedTasks(
          int index, com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureFailedTasksIsMutable();
        failedTasks_.set(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bytes failedTasks = 1;</code>
       */
      public Builder addFailedTasks(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureFailedTasksIsMutable();
        failedTasks_.add(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bytes failedTasks = 1;</code>
       */
      public Builder addAllFailedTasks(
          java.lang.Iterable<? extends com.google.protobuf.ByteString> values) {
        ensureFailedTasksIsMutable();
        super.addAll(values, failedTasks_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bytes failedTasks = 1;</code>
       */
      public Builder clearFailedTasks() {
        failedTasks_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:SuccessFilterMessage)
    }

    static {
      defaultInstance = new SuccessFilterMessage(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:SuccessFilterMessage)
  }

  public interface SkippingScanFilterMessageOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // repeated bytes startKeys = 1;
    /**
     * <code>repeated bytes startKeys = 1;</code>
     */
    java.util.List<com.google.protobuf.ByteString> getStartKeysList();
    /**
     * <code>repeated bytes startKeys = 1;</code>
     */
    int getStartKeysCount();
    /**
     * <code>repeated bytes startKeys = 1;</code>
     */
    com.google.protobuf.ByteString getStartKeys(int index);

    // repeated bytes stopKeys = 2;
    /**
     * <code>repeated bytes stopKeys = 2;</code>
     */
    java.util.List<com.google.protobuf.ByteString> getStopKeysList();
    /**
     * <code>repeated bytes stopKeys = 2;</code>
     */
    int getStopKeysCount();
    /**
     * <code>repeated bytes stopKeys = 2;</code>
     */
    com.google.protobuf.ByteString getStopKeys(int index);

    // repeated bytes predicates = 3;
    /**
     * <code>repeated bytes predicates = 3;</code>
     */
    java.util.List<com.google.protobuf.ByteString> getPredicatesList();
    /**
     * <code>repeated bytes predicates = 3;</code>
     */
    int getPredicatesCount();
    /**
     * <code>repeated bytes predicates = 3;</code>
     */
    com.google.protobuf.ByteString getPredicates(int index);
  }
  /**
   * Protobuf type {@code SkippingScanFilterMessage}
   */
  public static final class SkippingScanFilterMessage extends
      com.google.protobuf.GeneratedMessage
      implements SkippingScanFilterMessageOrBuilder {
    // Use SkippingScanFilterMessage.newBuilder() to construct.
    private SkippingScanFilterMessage(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private SkippingScanFilterMessage(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final SkippingScanFilterMessage defaultInstance;
    public static SkippingScanFilterMessage getDefaultInstance() {
      return defaultInstance;
    }

    public SkippingScanFilterMessage getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private SkippingScanFilterMessage(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              if (!((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
                startKeys_ = new java.util.ArrayList<com.google.protobuf.ByteString>();
                mutable_bitField0_ |= 0x00000001;
              }
              startKeys_.add(input.readBytes());
              break;
            }
            case 18: {
              if (!((mutable_bitField0_ & 0x00000002) == 0x00000002)) {
                stopKeys_ = new java.util.ArrayList<com.google.protobuf.ByteString>();
                mutable_bitField0_ |= 0x00000002;
              }
              stopKeys_.add(input.readBytes());
              break;
            }
            case 26: {
              if (!((mutable_bitField0_ & 0x00000004) == 0x00000004)) {
                predicates_ = new java.util.ArrayList<com.google.protobuf.ByteString>();
                mutable_bitField0_ |= 0x00000004;
              }
              predicates_.add(input.readBytes());
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        if (((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
          startKeys_ = java.util.Collections.unmodifiableList(startKeys_);
        }
        if (((mutable_bitField0_ & 0x00000002) == 0x00000002)) {
          stopKeys_ = java.util.Collections.unmodifiableList(stopKeys_);
        }
        if (((mutable_bitField0_ & 0x00000004) == 0x00000004)) {
          predicates_ = java.util.Collections.unmodifiableList(predicates_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_SkippingScanFilterMessage_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_SkippingScanFilterMessage_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage.class, com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage.Builder.class);
    }

    public static com.google.protobuf.Parser<SkippingScanFilterMessage> PARSER =
        new com.google.protobuf.AbstractParser<SkippingScanFilterMessage>() {
      public SkippingScanFilterMessage parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new SkippingScanFilterMessage(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<SkippingScanFilterMessage> getParserForType() {
      return PARSER;
    }

    // repeated bytes startKeys = 1;
    public static final int STARTKEYS_FIELD_NUMBER = 1;
    private java.util.List<com.google.protobuf.ByteString> startKeys_;
    /**
     * <code>repeated bytes startKeys = 1;</code>
     */
    public java.util.List<com.google.protobuf.ByteString>
        getStartKeysList() {
      return startKeys_;
    }
    /**
     * <code>repeated bytes startKeys = 1;</code>
     */
    public int getStartKeysCount() {
      return startKeys_.size();
    }
    /**
     * <code>repeated bytes startKeys = 1;</code>
     */
    public com.google.protobuf.ByteString getStartKeys(int index) {
      return startKeys_.get(index);
    }

    // repeated bytes stopKeys = 2;
    public static final int STOPKEYS_FIELD_NUMBER = 2;
    private java.util.List<com.google.protobuf.ByteString> stopKeys_;
    /**
     * <code>repeated bytes stopKeys = 2;</code>
     */
    public java.util.List<com.google.protobuf.ByteString>
        getStopKeysList() {
      return stopKeys_;
    }
    /**
     * <code>repeated bytes stopKeys = 2;</code>
     */
    public int getStopKeysCount() {
      return stopKeys_.size();
    }
    /**
     * <code>repeated bytes stopKeys = 2;</code>
     */
    public com.google.protobuf.ByteString getStopKeys(int index) {
      return stopKeys_.get(index);
    }

    // repeated bytes predicates = 3;
    public static final int PREDICATES_FIELD_NUMBER = 3;
    private java.util.List<com.google.protobuf.ByteString> predicates_;
    /**
     * <code>repeated bytes predicates = 3;</code>
     */
    public java.util.List<com.google.protobuf.ByteString>
        getPredicatesList() {
      return predicates_;
    }
    /**
     * <code>repeated bytes predicates = 3;</code>
     */
    public int getPredicatesCount() {
      return predicates_.size();
    }
    /**
     * <code>repeated bytes predicates = 3;</code>
     */
    public com.google.protobuf.ByteString getPredicates(int index) {
      return predicates_.get(index);
    }

    private void initFields() {
      startKeys_ = java.util.Collections.emptyList();
      stopKeys_ = java.util.Collections.emptyList();
      predicates_ = java.util.Collections.emptyList();
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      for (int i = 0; i < startKeys_.size(); i++) {
        output.writeBytes(1, startKeys_.get(i));
      }
      for (int i = 0; i < stopKeys_.size(); i++) {
        output.writeBytes(2, stopKeys_.get(i));
      }
      for (int i = 0; i < predicates_.size(); i++) {
        output.writeBytes(3, predicates_.get(i));
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      {
        int dataSize = 0;
        for (int i = 0; i < startKeys_.size(); i++) {
          dataSize += com.google.protobuf.CodedOutputStream
            .computeBytesSizeNoTag(startKeys_.get(i));
        }
        size += dataSize;
        size += 1 * getStartKeysList().size();
      }
      {
        int dataSize = 0;
        for (int i = 0; i < stopKeys_.size(); i++) {
          dataSize += com.google.protobuf.CodedOutputStream
            .computeBytesSizeNoTag(stopKeys_.get(i));
        }
        size += dataSize;
        size += 1 * getStopKeysList().size();
      }
      {
        int dataSize = 0;
        for (int i = 0; i < predicates_.size(); i++) {
          dataSize += com.google.protobuf.CodedOutputStream
            .computeBytesSizeNoTag(predicates_.get(i));
        }
        size += dataSize;
        size += 1 * getPredicatesList().size();
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage)) {
        return super.equals(obj);
      }
      com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage other = (com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage) obj;

      boolean result = true;
      result = result && getStartKeysList()
          .equals(other.getStartKeysList());
      result = result && getStopKeysList()
          .equals(other.getStopKeysList());
      result = result && getPredicatesList()
          .equals(other.getPredicatesList());
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }

    private int memoizedHashCode = 0;
    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      if (getStartKeysCount() > 0) {
        hash = (37 * hash) + STARTKEYS_FIELD_NUMBER;
        hash = (53 * hash) + getStartKeysList().hashCode();
      }
      if (getStopKeysCount() > 0) {
        hash = (37 * hash) + STOPKEYS_FIELD_NUMBER;
        hash = (53 * hash) + getStopKeysList().hashCode();
      }
      if (getPredicatesCount() > 0) {
        hash = (37 * hash) + PREDICATES_FIELD_NUMBER;
        hash = (53 * hash) + getPredicatesList().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code SkippingScanFilterMessage}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessageOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_SkippingScanFilterMessage_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_SkippingScanFilterMessage_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage.class, com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage.Builder.class);
      }

      // Construct using com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        startKeys_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
        stopKeys_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000002);
        predicates_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_SkippingScanFilterMessage_descriptor;
      }

      public com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage getDefaultInstanceForType() {
        return com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage.getDefaultInstance();
      }

      public com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage build() {
        com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage buildPartial() {
        com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage result = new com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage(this);
        int from_bitField0_ = bitField0_;
        if (((bitField0_ & 0x00000001) == 0x00000001)) {
          startKeys_ = java.util.Collections.unmodifiableList(startKeys_);
          bitField0_ = (bitField0_ & ~0x00000001);
        }
        result.startKeys_ = startKeys_;
        if (((bitField0_ & 0x00000002) == 0x00000002)) {
          stopKeys_ = java.util.Collections.unmodifiableList(stopKeys_);
          bitField0_ = (bitField0_ & ~0x00000002);
        }
        result.stopKeys_ = stopKeys_;
        if (((bitField0_ & 0x00000004) == 0x00000004)) {
          predicates_ = java.util.Collections.unmodifiableList(predicates_);
          bitField0_ = (bitField0_ & ~0x00000004);
        }
        result.predicates_ = predicates_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage) {
          return mergeFrom((com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage other) {
        if (other == com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage.getDefaultInstance()) return this;
        if (!other.startKeys_.isEmpty()) {
          if (startKeys_.isEmpty()) {
            startKeys_ = other.startKeys_;
            bitField0_ = (bitField0_ & ~0x00000001);
          } else {
            ensureStartKeysIsMutable();
            startKeys_.addAll(other.startKeys_);
          }
          onChanged();
        }
        if (!other.stopKeys_.isEmpty()) {
          if (stopKeys_.isEmpty()) {
            stopKeys_ = other.stopKeys_;
            bitField0_ = (bitField0_ & ~0x00000002);
          } else {
            ensureStopKeysIsMutable();
            stopKeys_.addAll(other.stopKeys_);
          }
          onChanged();
        }
        if (!other.predicates_.isEmpty()) {
          if (predicates_.isEmpty()) {
            predicates_ = other.predicates_;
            bitField0_ = (bitField0_ & ~0x00000004);
          } else {
            ensurePredicatesIsMutable();
            predicates_.addAll(other.predicates_);
          }
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.splicemachine.coprocessor.SpliceMessage.SkippingScanFilterMessage) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // repeated bytes startKeys = 1;
      private java.util.List<com.google.protobuf.ByteString> startKeys_ = java.util.Collections.emptyList();
      private void ensureStartKeysIsMutable() {
        if (!((bitField0_ & 0x00000001) == 0x00000001)) {
          startKeys_ = new java.util.ArrayList<com.google.protobuf.ByteString>(startKeys_);
          bitField0_ |= 0x00000001;
         }
      }
      /**
       * <code>repeated bytes startKeys = 1;</code>
       */
      public java.util.List<com.google.protobuf.ByteString>
          getStartKeysList() {
        return java.util.Collections.unmodifiableList(startKeys_);
      }
      /**
       * <code>repeated bytes startKeys = 1;</code>
       */
      public int getStartKeysCount() {
        return startKeys_.size();
      }
      /**
       * <code>repeated bytes startKeys = 1;</code>
       */
      public com.google.protobuf.ByteString getStartKeys(int index) {
        return startKeys_.get(index);
      }
      /**
       * <code>repeated bytes startKeys = 1;</code>
       */
      public Builder setStartKeys(
          int index, com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureStartKeysIsMutable();
        startKeys_.set(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bytes startKeys = 1;</code>
       */
      public Builder addStartKeys(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureStartKeysIsMutable();
        startKeys_.add(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bytes startKeys = 1;</code>
       */
      public Builder addAllStartKeys(
          java.lang.Iterable<? extends com.google.protobuf.ByteString> values) {
        ensureStartKeysIsMutable();
        super.addAll(values, startKeys_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bytes startKeys = 1;</code>
       */
      public Builder clearStartKeys() {
        startKeys_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
        return this;
      }

      // repeated bytes stopKeys = 2;
      private java.util.List<com.google.protobuf.ByteString> stopKeys_ = java.util.Collections.emptyList();
      private void ensureStopKeysIsMutable() {
        if (!((bitField0_ & 0x00000002) == 0x00000002)) {
          stopKeys_ = new java.util.ArrayList<com.google.protobuf.ByteString>(stopKeys_);
          bitField0_ |= 0x00000002;
         }
      }
      /**
       * <code>repeated bytes stopKeys = 2;</code>
       */
      public java.util.List<com.google.protobuf.ByteString>
          getStopKeysList() {
        return java.util.Collections.unmodifiableList(stopKeys_);
      }
      /**
       * <code>repeated bytes stopKeys = 2;</code>
       */
      public int getStopKeysCount() {
        return stopKeys_.size();
      }
      /**
       * <code>repeated bytes stopKeys = 2;</code>
       */
      public com.google.protobuf.ByteString getStopKeys(int index) {
        return stopKeys_.get(index);
      }
      /**
       * <code>repeated bytes stopKeys = 2;</code>
       */
      public Builder setStopKeys(
          int index, com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureStopKeysIsMutable();
        stopKeys_.set(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bytes stopKeys = 2;</code>
       */
      public Builder addStopKeys(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureStopKeysIsMutable();
        stopKeys_.add(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bytes stopKeys = 2;</code>
       */
      public Builder addAllStopKeys(
          java.lang.Iterable<? extends com.google.protobuf.ByteString> values) {
        ensureStopKeysIsMutable();
        super.addAll(values, stopKeys_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bytes stopKeys = 2;</code>
       */
      public Builder clearStopKeys() {
        stopKeys_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000002);
        onChanged();
        return this;
      }

      // repeated bytes predicates = 3;
      private java.util.List<com.google.protobuf.ByteString> predicates_ = java.util.Collections.emptyList();
      private void ensurePredicatesIsMutable() {
        if (!((bitField0_ & 0x00000004) == 0x00000004)) {
          predicates_ = new java.util.ArrayList<com.google.protobuf.ByteString>(predicates_);
          bitField0_ |= 0x00000004;
         }
      }
      /**
       * <code>repeated bytes predicates = 3;</code>
       */
      public java.util.List<com.google.protobuf.ByteString>
          getPredicatesList() {
        return java.util.Collections.unmodifiableList(predicates_);
      }
      /**
       * <code>repeated bytes predicates = 3;</code>
       */
      public int getPredicatesCount() {
        return predicates_.size();
      }
      /**
       * <code>repeated bytes predicates = 3;</code>
       */
      public com.google.protobuf.ByteString getPredicates(int index) {
        return predicates_.get(index);
      }
      /**
       * <code>repeated bytes predicates = 3;</code>
       */
      public Builder setPredicates(
          int index, com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensurePredicatesIsMutable();
        predicates_.set(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bytes predicates = 3;</code>
       */
      public Builder addPredicates(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensurePredicatesIsMutable();
        predicates_.add(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bytes predicates = 3;</code>
       */
      public Builder addAllPredicates(
          java.lang.Iterable<? extends com.google.protobuf.ByteString> values) {
        ensurePredicatesIsMutable();
        super.addAll(values, predicates_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bytes predicates = 3;</code>
       */
      public Builder clearPredicates() {
        predicates_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000004);
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:SkippingScanFilterMessage)
    }

    static {
      defaultInstance = new SkippingScanFilterMessage(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:SkippingScanFilterMessage)
  }

  public interface HbaseAttributeHolderMessageOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;
    /**
     * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
     */
    java.util.List<com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute> 
        getAttributesList();
    /**
     * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
     */
    com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute getAttributes(int index);
    /**
     * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
     */
    int getAttributesCount();
    /**
     * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
     */
    java.util.List<? extends com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.AttributeOrBuilder> 
        getAttributesOrBuilderList();
    /**
     * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
     */
    com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.AttributeOrBuilder getAttributesOrBuilder(
        int index);
  }
  /**
   * Protobuf type {@code HbaseAttributeHolderMessage}
   */
  public static final class HbaseAttributeHolderMessage extends
      com.google.protobuf.GeneratedMessage
      implements HbaseAttributeHolderMessageOrBuilder {
    // Use HbaseAttributeHolderMessage.newBuilder() to construct.
    private HbaseAttributeHolderMessage(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private HbaseAttributeHolderMessage(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final HbaseAttributeHolderMessage defaultInstance;
    public static HbaseAttributeHolderMessage getDefaultInstance() {
      return defaultInstance;
    }

    public HbaseAttributeHolderMessage getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private HbaseAttributeHolderMessage(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              if (!((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
                attributes_ = new java.util.ArrayList<com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute>();
                mutable_bitField0_ |= 0x00000001;
              }
              attributes_.add(input.readMessage(com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.PARSER, extensionRegistry));
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        if (((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
          attributes_ = java.util.Collections.unmodifiableList(attributes_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_HbaseAttributeHolderMessage_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_HbaseAttributeHolderMessage_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.class, com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Builder.class);
    }

    public static com.google.protobuf.Parser<HbaseAttributeHolderMessage> PARSER =
        new com.google.protobuf.AbstractParser<HbaseAttributeHolderMessage>() {
      public HbaseAttributeHolderMessage parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new HbaseAttributeHolderMessage(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<HbaseAttributeHolderMessage> getParserForType() {
      return PARSER;
    }

    public interface AttributeOrBuilder
        extends com.google.protobuf.MessageOrBuilder {

      // required string name = 1;
      /**
       * <code>required string name = 1;</code>
       */
      boolean hasName();
      /**
       * <code>required string name = 1;</code>
       */
      java.lang.String getName();
      /**
       * <code>required string name = 1;</code>
       */
      com.google.protobuf.ByteString
          getNameBytes();

      // required bytes value = 2;
      /**
       * <code>required bytes value = 2;</code>
       */
      boolean hasValue();
      /**
       * <code>required bytes value = 2;</code>
       */
      com.google.protobuf.ByteString getValue();
    }
    /**
     * Protobuf type {@code HbaseAttributeHolderMessage.Attribute}
     */
    public static final class Attribute extends
        com.google.protobuf.GeneratedMessage
        implements AttributeOrBuilder {
      // Use Attribute.newBuilder() to construct.
      private Attribute(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
        super(builder);
        this.unknownFields = builder.getUnknownFields();
      }
      private Attribute(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

      private static final Attribute defaultInstance;
      public static Attribute getDefaultInstance() {
        return defaultInstance;
      }

      public Attribute getDefaultInstanceForType() {
        return defaultInstance;
      }

      private final com.google.protobuf.UnknownFieldSet unknownFields;
      @java.lang.Override
      public final com.google.protobuf.UnknownFieldSet
          getUnknownFields() {
        return this.unknownFields;
      }
      private Attribute(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        initFields();
        int mutable_bitField0_ = 0;
        com.google.protobuf.UnknownFieldSet.Builder unknownFields =
            com.google.protobuf.UnknownFieldSet.newBuilder();
        try {
          boolean done = false;
          while (!done) {
            int tag = input.readTag();
            switch (tag) {
              case 0:
                done = true;
                break;
              default: {
                if (!parseUnknownField(input, unknownFields,
                                       extensionRegistry, tag)) {
                  done = true;
                }
                break;
              }
              case 10: {
                bitField0_ |= 0x00000001;
                name_ = input.readBytes();
                break;
              }
              case 18: {
                bitField0_ |= 0x00000002;
                value_ = input.readBytes();
                break;
              }
            }
          }
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          throw e.setUnfinishedMessage(this);
        } catch (java.io.IOException e) {
          throw new com.google.protobuf.InvalidProtocolBufferException(
              e.getMessage()).setUnfinishedMessage(this);
        } finally {
          this.unknownFields = unknownFields.build();
          makeExtensionsImmutable();
        }
      }
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_HbaseAttributeHolderMessage_Attribute_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_HbaseAttributeHolderMessage_Attribute_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.class, com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.Builder.class);
      }

      public static com.google.protobuf.Parser<Attribute> PARSER =
          new com.google.protobuf.AbstractParser<Attribute>() {
        public Attribute parsePartialFrom(
            com.google.protobuf.CodedInputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
          return new Attribute(input, extensionRegistry);
        }
      };

      @java.lang.Override
      public com.google.protobuf.Parser<Attribute> getParserForType() {
        return PARSER;
      }

      private int bitField0_;
      // required string name = 1;
      public static final int NAME_FIELD_NUMBER = 1;
      private java.lang.Object name_;
      /**
       * <code>required string name = 1;</code>
       */
      public boolean hasName() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required string name = 1;</code>
       */
      public java.lang.String getName() {
        java.lang.Object ref = name_;
        if (ref instanceof java.lang.String) {
          return (java.lang.String) ref;
        } else {
          com.google.protobuf.ByteString bs = 
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            name_ = s;
          }
          return s;
        }
      }
      /**
       * <code>required string name = 1;</code>
       */
      public com.google.protobuf.ByteString
          getNameBytes() {
        java.lang.Object ref = name_;
        if (ref instanceof java.lang.String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          name_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }

      // required bytes value = 2;
      public static final int VALUE_FIELD_NUMBER = 2;
      private com.google.protobuf.ByteString value_;
      /**
       * <code>required bytes value = 2;</code>
       */
      public boolean hasValue() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>required bytes value = 2;</code>
       */
      public com.google.protobuf.ByteString getValue() {
        return value_;
      }

      private void initFields() {
        name_ = "";
        value_ = com.google.protobuf.ByteString.EMPTY;
      }
      private byte memoizedIsInitialized = -1;
      public final boolean isInitialized() {
        byte isInitialized = memoizedIsInitialized;
        if (isInitialized != -1) return isInitialized == 1;

        if (!hasName()) {
          memoizedIsInitialized = 0;
          return false;
        }
        if (!hasValue()) {
          memoizedIsInitialized = 0;
          return false;
        }
        memoizedIsInitialized = 1;
        return true;
      }

      public void writeTo(com.google.protobuf.CodedOutputStream output)
                          throws java.io.IOException {
        getSerializedSize();
        if (((bitField0_ & 0x00000001) == 0x00000001)) {
          output.writeBytes(1, getNameBytes());
        }
        if (((bitField0_ & 0x00000002) == 0x00000002)) {
          output.writeBytes(2, value_);
        }
        getUnknownFields().writeTo(output);
      }

      private int memoizedSerializedSize = -1;
      public int getSerializedSize() {
        int size = memoizedSerializedSize;
        if (size != -1) return size;

        size = 0;
        if (((bitField0_ & 0x00000001) == 0x00000001)) {
          size += com.google.protobuf.CodedOutputStream
            .computeBytesSize(1, getNameBytes());
        }
        if (((bitField0_ & 0x00000002) == 0x00000002)) {
          size += com.google.protobuf.CodedOutputStream
            .computeBytesSize(2, value_);
        }
        size += getUnknownFields().getSerializedSize();
        memoizedSerializedSize = size;
        return size;
      }

      private static final long serialVersionUID = 0L;
      @java.lang.Override
      protected java.lang.Object writeReplace()
          throws java.io.ObjectStreamException {
        return super.writeReplace();
      }

      @java.lang.Override
      public boolean equals(final java.lang.Object obj) {
        if (obj == this) {
         return true;
        }
        if (!(obj instanceof com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute)) {
          return super.equals(obj);
        }
        com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute other = (com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute) obj;

        boolean result = true;
        result = result && (hasName() == other.hasName());
        if (hasName()) {
          result = result && getName()
              .equals(other.getName());
        }
        result = result && (hasValue() == other.hasValue());
        if (hasValue()) {
          result = result && getValue()
              .equals(other.getValue());
        }
        result = result &&
            getUnknownFields().equals(other.getUnknownFields());
        return result;
      }

      private int memoizedHashCode = 0;
      @java.lang.Override
      public int hashCode() {
        if (memoizedHashCode != 0) {
          return memoizedHashCode;
        }
        int hash = 41;
        hash = (19 * hash) + getDescriptorForType().hashCode();
        if (hasName()) {
          hash = (37 * hash) + NAME_FIELD_NUMBER;
          hash = (53 * hash) + getName().hashCode();
        }
        if (hasValue()) {
          hash = (37 * hash) + VALUE_FIELD_NUMBER;
          hash = (53 * hash) + getValue().hashCode();
        }
        hash = (29 * hash) + getUnknownFields().hashCode();
        memoizedHashCode = hash;
        return hash;
      }

      public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute parseFrom(
          com.google.protobuf.ByteString data)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data);
      }
      public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute parseFrom(
          com.google.protobuf.ByteString data,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data, extensionRegistry);
      }
      public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute parseFrom(byte[] data)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data);
      }
      public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute parseFrom(
          byte[] data,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data, extensionRegistry);
      }
      public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute parseFrom(java.io.InputStream input)
          throws java.io.IOException {
        return PARSER.parseFrom(input);
      }
      public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute parseFrom(
          java.io.InputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        return PARSER.parseFrom(input, extensionRegistry);
      }
      public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute parseDelimitedFrom(java.io.InputStream input)
          throws java.io.IOException {
        return PARSER.parseDelimitedFrom(input);
      }
      public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute parseDelimitedFrom(
          java.io.InputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        return PARSER.parseDelimitedFrom(input, extensionRegistry);
      }
      public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute parseFrom(
          com.google.protobuf.CodedInputStream input)
          throws java.io.IOException {
        return PARSER.parseFrom(input);
      }
      public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute parseFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        return PARSER.parseFrom(input, extensionRegistry);
      }

      public static Builder newBuilder() { return Builder.create(); }
      public Builder newBuilderForType() { return newBuilder(); }
      public static Builder newBuilder(com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute prototype) {
        return newBuilder().mergeFrom(prototype);
      }
      public Builder toBuilder() { return newBuilder(this); }

      @java.lang.Override
      protected Builder newBuilderForType(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        Builder builder = new Builder(parent);
        return builder;
      }
      /**
       * Protobuf type {@code HbaseAttributeHolderMessage.Attribute}
       */
      public static final class Builder extends
          com.google.protobuf.GeneratedMessage.Builder<Builder>
         implements com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.AttributeOrBuilder {
        public static final com.google.protobuf.Descriptors.Descriptor
            getDescriptor() {
          return com.splicemachine.coprocessor.SpliceMessage.internal_static_HbaseAttributeHolderMessage_Attribute_descriptor;
        }

        protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
            internalGetFieldAccessorTable() {
          return com.splicemachine.coprocessor.SpliceMessage.internal_static_HbaseAttributeHolderMessage_Attribute_fieldAccessorTable
              .ensureFieldAccessorsInitialized(
                  com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.class, com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.Builder.class);
        }

        // Construct using com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.newBuilder()
        private Builder() {
          maybeForceBuilderInitialization();
        }

        private Builder(
            com.google.protobuf.GeneratedMessage.BuilderParent parent) {
          super(parent);
          maybeForceBuilderInitialization();
        }
        private void maybeForceBuilderInitialization() {
          if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
          }
        }
        private static Builder create() {
          return new Builder();
        }

        public Builder clear() {
          super.clear();
          name_ = "";
          bitField0_ = (bitField0_ & ~0x00000001);
          value_ = com.google.protobuf.ByteString.EMPTY;
          bitField0_ = (bitField0_ & ~0x00000002);
          return this;
        }

        public Builder clone() {
          return create().mergeFrom(buildPartial());
        }

        public com.google.protobuf.Descriptors.Descriptor
            getDescriptorForType() {
          return com.splicemachine.coprocessor.SpliceMessage.internal_static_HbaseAttributeHolderMessage_Attribute_descriptor;
        }

        public com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute getDefaultInstanceForType() {
          return com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.getDefaultInstance();
        }

        public com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute build() {
          com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute result = buildPartial();
          if (!result.isInitialized()) {
            throw newUninitializedMessageException(result);
          }
          return result;
        }

        public com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute buildPartial() {
          com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute result = new com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute(this);
          int from_bitField0_ = bitField0_;
          int to_bitField0_ = 0;
          if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
            to_bitField0_ |= 0x00000001;
          }
          result.name_ = name_;
          if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
            to_bitField0_ |= 0x00000002;
          }
          result.value_ = value_;
          result.bitField0_ = to_bitField0_;
          onBuilt();
          return result;
        }

        public Builder mergeFrom(com.google.protobuf.Message other) {
          if (other instanceof com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute) {
            return mergeFrom((com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute)other);
          } else {
            super.mergeFrom(other);
            return this;
          }
        }

        public Builder mergeFrom(com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute other) {
          if (other == com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.getDefaultInstance()) return this;
          if (other.hasName()) {
            bitField0_ |= 0x00000001;
            name_ = other.name_;
            onChanged();
          }
          if (other.hasValue()) {
            setValue(other.getValue());
          }
          this.mergeUnknownFields(other.getUnknownFields());
          return this;
        }

        public final boolean isInitialized() {
          if (!hasName()) {
            
            return false;
          }
          if (!hasValue()) {
            
            return false;
          }
          return true;
        }

        public Builder mergeFrom(
            com.google.protobuf.CodedInputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
          com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute parsedMessage = null;
          try {
            parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
          } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            parsedMessage = (com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute) e.getUnfinishedMessage();
            throw e;
          } finally {
            if (parsedMessage != null) {
              mergeFrom(parsedMessage);
            }
          }
          return this;
        }
        private int bitField0_;

        // required string name = 1;
        private java.lang.Object name_ = "";
        /**
         * <code>required string name = 1;</code>
         */
        public boolean hasName() {
          return ((bitField0_ & 0x00000001) == 0x00000001);
        }
        /**
         * <code>required string name = 1;</code>
         */
        public java.lang.String getName() {
          java.lang.Object ref = name_;
          if (!(ref instanceof java.lang.String)) {
            java.lang.String s = ((com.google.protobuf.ByteString) ref)
                .toStringUtf8();
            name_ = s;
            return s;
          } else {
            return (java.lang.String) ref;
          }
        }
        /**
         * <code>required string name = 1;</code>
         */
        public com.google.protobuf.ByteString
            getNameBytes() {
          java.lang.Object ref = name_;
          if (ref instanceof String) {
            com.google.protobuf.ByteString b = 
                com.google.protobuf.ByteString.copyFromUtf8(
                    (java.lang.String) ref);
            name_ = b;
            return b;
          } else {
            return (com.google.protobuf.ByteString) ref;
          }
        }
        /**
         * <code>required string name = 1;</code>
         */
        public Builder setName(
            java.lang.String value) {
          if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
          name_ = value;
          onChanged();
          return this;
        }
        /**
         * <code>required string name = 1;</code>
         */
        public Builder clearName() {
          bitField0_ = (bitField0_ & ~0x00000001);
          name_ = getDefaultInstance().getName();
          onChanged();
          return this;
        }
        /**
         * <code>required string name = 1;</code>
         */
        public Builder setNameBytes(
            com.google.protobuf.ByteString value) {
          if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
          name_ = value;
          onChanged();
          return this;
        }

        // required bytes value = 2;
        private com.google.protobuf.ByteString value_ = com.google.protobuf.ByteString.EMPTY;
        /**
         * <code>required bytes value = 2;</code>
         */
        public boolean hasValue() {
          return ((bitField0_ & 0x00000002) == 0x00000002);
        }
        /**
         * <code>required bytes value = 2;</code>
         */
        public com.google.protobuf.ByteString getValue() {
          return value_;
        }
        /**
         * <code>required bytes value = 2;</code>
         */
        public Builder setValue(com.google.protobuf.ByteString value) {
          if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
          value_ = value;
          onChanged();
          return this;
        }
        /**
         * <code>required bytes value = 2;</code>
         */
        public Builder clearValue() {
          bitField0_ = (bitField0_ & ~0x00000002);
          value_ = getDefaultInstance().getValue();
          onChanged();
          return this;
        }

        // @@protoc_insertion_point(builder_scope:HbaseAttributeHolderMessage.Attribute)
      }

      static {
        defaultInstance = new Attribute(true);
        defaultInstance.initFields();
      }

      // @@protoc_insertion_point(class_scope:HbaseAttributeHolderMessage.Attribute)
    }

    // repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;
    public static final int ATTRIBUTES_FIELD_NUMBER = 1;
    private java.util.List<com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute> attributes_;
    /**
     * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
     */
    public java.util.List<com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute> getAttributesList() {
      return attributes_;
    }
    /**
     * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
     */
    public java.util.List<? extends com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.AttributeOrBuilder> 
        getAttributesOrBuilderList() {
      return attributes_;
    }
    /**
     * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
     */
    public int getAttributesCount() {
      return attributes_.size();
    }
    /**
     * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
     */
    public com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute getAttributes(int index) {
      return attributes_.get(index);
    }
    /**
     * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
     */
    public com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.AttributeOrBuilder getAttributesOrBuilder(
        int index) {
      return attributes_.get(index);
    }

    private void initFields() {
      attributes_ = java.util.Collections.emptyList();
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      for (int i = 0; i < getAttributesCount(); i++) {
        if (!getAttributes(i).isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      for (int i = 0; i < attributes_.size(); i++) {
        output.writeMessage(1, attributes_.get(i));
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      for (int i = 0; i < attributes_.size(); i++) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(1, attributes_.get(i));
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage)) {
        return super.equals(obj);
      }
      com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage other = (com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage) obj;

      boolean result = true;
      result = result && getAttributesList()
          .equals(other.getAttributesList());
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }

    private int memoizedHashCode = 0;
    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      if (getAttributesCount() > 0) {
        hash = (37 * hash) + ATTRIBUTES_FIELD_NUMBER;
        hash = (53 * hash) + getAttributesList().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code HbaseAttributeHolderMessage}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessageOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_HbaseAttributeHolderMessage_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_HbaseAttributeHolderMessage_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.class, com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Builder.class);
      }

      // Construct using com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
          getAttributesFieldBuilder();
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        if (attributesBuilder_ == null) {
          attributes_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000001);
        } else {
          attributesBuilder_.clear();
        }
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_HbaseAttributeHolderMessage_descriptor;
      }

      public com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage getDefaultInstanceForType() {
        return com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.getDefaultInstance();
      }

      public com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage build() {
        com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage buildPartial() {
        com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage result = new com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage(this);
        int from_bitField0_ = bitField0_;
        if (attributesBuilder_ == null) {
          if (((bitField0_ & 0x00000001) == 0x00000001)) {
            attributes_ = java.util.Collections.unmodifiableList(attributes_);
            bitField0_ = (bitField0_ & ~0x00000001);
          }
          result.attributes_ = attributes_;
        } else {
          result.attributes_ = attributesBuilder_.build();
        }
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage) {
          return mergeFrom((com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage other) {
        if (other == com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.getDefaultInstance()) return this;
        if (attributesBuilder_ == null) {
          if (!other.attributes_.isEmpty()) {
            if (attributes_.isEmpty()) {
              attributes_ = other.attributes_;
              bitField0_ = (bitField0_ & ~0x00000001);
            } else {
              ensureAttributesIsMutable();
              attributes_.addAll(other.attributes_);
            }
            onChanged();
          }
        } else {
          if (!other.attributes_.isEmpty()) {
            if (attributesBuilder_.isEmpty()) {
              attributesBuilder_.dispose();
              attributesBuilder_ = null;
              attributes_ = other.attributes_;
              bitField0_ = (bitField0_ & ~0x00000001);
              attributesBuilder_ = 
                com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders ?
                   getAttributesFieldBuilder() : null;
            } else {
              attributesBuilder_.addAllMessages(other.attributes_);
            }
          }
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        for (int i = 0; i < getAttributesCount(); i++) {
          if (!getAttributes(i).isInitialized()) {
            
            return false;
          }
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;
      private java.util.List<com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute> attributes_ =
        java.util.Collections.emptyList();
      private void ensureAttributesIsMutable() {
        if (!((bitField0_ & 0x00000001) == 0x00000001)) {
          attributes_ = new java.util.ArrayList<com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute>(attributes_);
          bitField0_ |= 0x00000001;
         }
      }

      private com.google.protobuf.RepeatedFieldBuilder<
          com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute, com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.Builder, com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.AttributeOrBuilder> attributesBuilder_;

      /**
       * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
       */
      public java.util.List<com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute> getAttributesList() {
        if (attributesBuilder_ == null) {
          return java.util.Collections.unmodifiableList(attributes_);
        } else {
          return attributesBuilder_.getMessageList();
        }
      }
      /**
       * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
       */
      public int getAttributesCount() {
        if (attributesBuilder_ == null) {
          return attributes_.size();
        } else {
          return attributesBuilder_.getCount();
        }
      }
      /**
       * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
       */
      public com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute getAttributes(int index) {
        if (attributesBuilder_ == null) {
          return attributes_.get(index);
        } else {
          return attributesBuilder_.getMessage(index);
        }
      }
      /**
       * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
       */
      public Builder setAttributes(
          int index, com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute value) {
        if (attributesBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureAttributesIsMutable();
          attributes_.set(index, value);
          onChanged();
        } else {
          attributesBuilder_.setMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
       */
      public Builder setAttributes(
          int index, com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.Builder builderForValue) {
        if (attributesBuilder_ == null) {
          ensureAttributesIsMutable();
          attributes_.set(index, builderForValue.build());
          onChanged();
        } else {
          attributesBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
       */
      public Builder addAttributes(com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute value) {
        if (attributesBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureAttributesIsMutable();
          attributes_.add(value);
          onChanged();
        } else {
          attributesBuilder_.addMessage(value);
        }
        return this;
      }
      /**
       * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
       */
      public Builder addAttributes(
          int index, com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute value) {
        if (attributesBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureAttributesIsMutable();
          attributes_.add(index, value);
          onChanged();
        } else {
          attributesBuilder_.addMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
       */
      public Builder addAttributes(
          com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.Builder builderForValue) {
        if (attributesBuilder_ == null) {
          ensureAttributesIsMutable();
          attributes_.add(builderForValue.build());
          onChanged();
        } else {
          attributesBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
       */
      public Builder addAttributes(
          int index, com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.Builder builderForValue) {
        if (attributesBuilder_ == null) {
          ensureAttributesIsMutable();
          attributes_.add(index, builderForValue.build());
          onChanged();
        } else {
          attributesBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
       */
      public Builder addAllAttributes(
          java.lang.Iterable<? extends com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute> values) {
        if (attributesBuilder_ == null) {
          ensureAttributesIsMutable();
          super.addAll(values, attributes_);
          onChanged();
        } else {
          attributesBuilder_.addAllMessages(values);
        }
        return this;
      }
      /**
       * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
       */
      public Builder clearAttributes() {
        if (attributesBuilder_ == null) {
          attributes_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000001);
          onChanged();
        } else {
          attributesBuilder_.clear();
        }
        return this;
      }
      /**
       * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
       */
      public Builder removeAttributes(int index) {
        if (attributesBuilder_ == null) {
          ensureAttributesIsMutable();
          attributes_.remove(index);
          onChanged();
        } else {
          attributesBuilder_.remove(index);
        }
        return this;
      }
      /**
       * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
       */
      public com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.Builder getAttributesBuilder(
          int index) {
        return getAttributesFieldBuilder().getBuilder(index);
      }
      /**
       * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
       */
      public com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.AttributeOrBuilder getAttributesOrBuilder(
          int index) {
        if (attributesBuilder_ == null) {
          return attributes_.get(index);  } else {
          return attributesBuilder_.getMessageOrBuilder(index);
        }
      }
      /**
       * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
       */
      public java.util.List<? extends com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.AttributeOrBuilder> 
           getAttributesOrBuilderList() {
        if (attributesBuilder_ != null) {
          return attributesBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(attributes_);
        }
      }
      /**
       * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
       */
      public com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.Builder addAttributesBuilder() {
        return getAttributesFieldBuilder().addBuilder(
            com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.getDefaultInstance());
      }
      /**
       * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
       */
      public com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.Builder addAttributesBuilder(
          int index) {
        return getAttributesFieldBuilder().addBuilder(
            index, com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.getDefaultInstance());
      }
      /**
       * <code>repeated .HbaseAttributeHolderMessage.Attribute attributes = 1;</code>
       */
      public java.util.List<com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.Builder> 
           getAttributesBuilderList() {
        return getAttributesFieldBuilder().getBuilderList();
      }
      private com.google.protobuf.RepeatedFieldBuilder<
          com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute, com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.Builder, com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.AttributeOrBuilder> 
          getAttributesFieldBuilder() {
        if (attributesBuilder_ == null) {
          attributesBuilder_ = new com.google.protobuf.RepeatedFieldBuilder<
              com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute, com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.Attribute.Builder, com.splicemachine.coprocessor.SpliceMessage.HbaseAttributeHolderMessage.AttributeOrBuilder>(
                  attributes_,
                  ((bitField0_ & 0x00000001) == 0x00000001),
                  getParentForChildren(),
                  isClean());
          attributes_ = null;
        }
        return attributesBuilder_;
      }

      // @@protoc_insertion_point(builder_scope:HbaseAttributeHolderMessage)
    }

    static {
      defaultInstance = new HbaseAttributeHolderMessage(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:HbaseAttributeHolderMessage)
  }

  public interface WriteResultOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // optional .WriteResult.Code code = 1;
    /**
     * <code>optional .WriteResult.Code code = 1;</code>
     */
    boolean hasCode();
    /**
     * <code>optional .WriteResult.Code code = 1;</code>
     */
    com.splicemachine.coprocessor.SpliceMessage.WriteResult.Code getCode();

    // optional string errorMessage = 2;
    /**
     * <code>optional string errorMessage = 2;</code>
     */
    boolean hasErrorMessage();
    /**
     * <code>optional string errorMessage = 2;</code>
     */
    java.lang.String getErrorMessage();
    /**
     * <code>optional string errorMessage = 2;</code>
     */
    com.google.protobuf.ByteString
        getErrorMessageBytes();

    // optional .ConstraintContext constraintContext = 3;
    /**
     * <code>optional .ConstraintContext constraintContext = 3;</code>
     */
    boolean hasConstraintContext();
    /**
     * <code>optional .ConstraintContext constraintContext = 3;</code>
     */
    com.splicemachine.coprocessor.SpliceMessage.ConstraintContext getConstraintContext();
    /**
     * <code>optional .ConstraintContext constraintContext = 3;</code>
     */
    com.splicemachine.coprocessor.SpliceMessage.ConstraintContextOrBuilder getConstraintContextOrBuilder();
  }
  /**
   * Protobuf type {@code WriteResult}
   */
  public static final class WriteResult extends
      com.google.protobuf.GeneratedMessage
      implements WriteResultOrBuilder {
    // Use WriteResult.newBuilder() to construct.
    private WriteResult(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private WriteResult(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final WriteResult defaultInstance;
    public static WriteResult getDefaultInstance() {
      return defaultInstance;
    }

    public WriteResult getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private WriteResult(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 8: {
              int rawValue = input.readEnum();
              com.splicemachine.coprocessor.SpliceMessage.WriteResult.Code value = com.splicemachine.coprocessor.SpliceMessage.WriteResult.Code.valueOf(rawValue);
              if (value == null) {
                unknownFields.mergeVarintField(1, rawValue);
              } else {
                bitField0_ |= 0x00000001;
                code_ = value;
              }
              break;
            }
            case 18: {
              bitField0_ |= 0x00000002;
              errorMessage_ = input.readBytes();
              break;
            }
            case 26: {
              com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.Builder subBuilder = null;
              if (((bitField0_ & 0x00000004) == 0x00000004)) {
                subBuilder = constraintContext_.toBuilder();
              }
              constraintContext_ = input.readMessage(com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(constraintContext_);
                constraintContext_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000004;
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_WriteResult_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_WriteResult_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.splicemachine.coprocessor.SpliceMessage.WriteResult.class, com.splicemachine.coprocessor.SpliceMessage.WriteResult.Builder.class);
    }

    public static com.google.protobuf.Parser<WriteResult> PARSER =
        new com.google.protobuf.AbstractParser<WriteResult>() {
      public WriteResult parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new WriteResult(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<WriteResult> getParserForType() {
      return PARSER;
    }

    /**
     * Protobuf enum {@code WriteResult.Code}
     */
    public enum Code
        implements com.google.protobuf.ProtocolMessageEnum {
      /**
       * <code>FAILED = 0;</code>
       */
      FAILED(0, 0),
      /**
       * <code>WRITE_CONFLICT = 1;</code>
       */
      WRITE_CONFLICT(1, 1),
      /**
       * <code>SUCCESS = 2;</code>
       */
      SUCCESS(2, 2),
      /**
       * <code>PRIMARY_KEY_VIOLATION = 3;</code>
       */
      PRIMARY_KEY_VIOLATION(3, 3),
      /**
       * <code>UNIQUE_VIOLATION = 4;</code>
       */
      UNIQUE_VIOLATION(4, 4),
      /**
       * <code>FOREIGN_KEY_VIOLATION = 5;</code>
       */
      FOREIGN_KEY_VIOLATION(5, 5),
      /**
       * <code>CHECK_VIOLATION = 6;</code>
       */
      CHECK_VIOLATION(6, 6),
      /**
       * <code>NOT_SERVING_REGION = 7;</code>
       */
      NOT_SERVING_REGION(7, 7),
      /**
       * <code>WRONG_REGION = 8;</code>
       */
      WRONG_REGION(8, 8),
      /**
       * <code>REGION_TOO_BUSY = 9;</code>
       */
      REGION_TOO_BUSY(9, 9),
      /**
       * <code>NOT_RUN = 10;</code>
       */
      NOT_RUN(10, 10),
      /**
       * <code>NOT_NULL = 11;</code>
       */
      NOT_NULL(11, 11),
      ;

      /**
       * <code>FAILED = 0;</code>
       */
      public static final int FAILED_VALUE = 0;
      /**
       * <code>WRITE_CONFLICT = 1;</code>
       */
      public static final int WRITE_CONFLICT_VALUE = 1;
      /**
       * <code>SUCCESS = 2;</code>
       */
      public static final int SUCCESS_VALUE = 2;
      /**
       * <code>PRIMARY_KEY_VIOLATION = 3;</code>
       */
      public static final int PRIMARY_KEY_VIOLATION_VALUE = 3;
      /**
       * <code>UNIQUE_VIOLATION = 4;</code>
       */
      public static final int UNIQUE_VIOLATION_VALUE = 4;
      /**
       * <code>FOREIGN_KEY_VIOLATION = 5;</code>
       */
      public static final int FOREIGN_KEY_VIOLATION_VALUE = 5;
      /**
       * <code>CHECK_VIOLATION = 6;</code>
       */
      public static final int CHECK_VIOLATION_VALUE = 6;
      /**
       * <code>NOT_SERVING_REGION = 7;</code>
       */
      public static final int NOT_SERVING_REGION_VALUE = 7;
      /**
       * <code>WRONG_REGION = 8;</code>
       */
      public static final int WRONG_REGION_VALUE = 8;
      /**
       * <code>REGION_TOO_BUSY = 9;</code>
       */
      public static final int REGION_TOO_BUSY_VALUE = 9;
      /**
       * <code>NOT_RUN = 10;</code>
       */
      public static final int NOT_RUN_VALUE = 10;
      /**
       * <code>NOT_NULL = 11;</code>
       */
      public static final int NOT_NULL_VALUE = 11;


      public final int getNumber() { return value; }

      public static Code valueOf(int value) {
        switch (value) {
          case 0: return FAILED;
          case 1: return WRITE_CONFLICT;
          case 2: return SUCCESS;
          case 3: return PRIMARY_KEY_VIOLATION;
          case 4: return UNIQUE_VIOLATION;
          case 5: return FOREIGN_KEY_VIOLATION;
          case 6: return CHECK_VIOLATION;
          case 7: return NOT_SERVING_REGION;
          case 8: return WRONG_REGION;
          case 9: return REGION_TOO_BUSY;
          case 10: return NOT_RUN;
          case 11: return NOT_NULL;
          default: return null;
        }
      }

      public static com.google.protobuf.Internal.EnumLiteMap<Code>
          internalGetValueMap() {
        return internalValueMap;
      }
      private static com.google.protobuf.Internal.EnumLiteMap<Code>
          internalValueMap =
            new com.google.protobuf.Internal.EnumLiteMap<Code>() {
              public Code findValueByNumber(int number) {
                return Code.valueOf(number);
              }
            };

      public final com.google.protobuf.Descriptors.EnumValueDescriptor
          getValueDescriptor() {
        return getDescriptor().getValues().get(index);
      }
      public final com.google.protobuf.Descriptors.EnumDescriptor
          getDescriptorForType() {
        return getDescriptor();
      }
      public static final com.google.protobuf.Descriptors.EnumDescriptor
          getDescriptor() {
        return com.splicemachine.coprocessor.SpliceMessage.WriteResult.getDescriptor().getEnumTypes().get(0);
      }

      private static final Code[] VALUES = values();

      public static Code valueOf(
          com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
        if (desc.getType() != getDescriptor()) {
          throw new java.lang.IllegalArgumentException(
            "EnumValueDescriptor is not for this type.");
        }
        return VALUES[desc.getIndex()];
      }

      private final int index;
      private final int value;

      private Code(int index, int value) {
        this.index = index;
        this.value = value;
      }

      // @@protoc_insertion_point(enum_scope:WriteResult.Code)
    }

    private int bitField0_;
    // optional .WriteResult.Code code = 1;
    public static final int CODE_FIELD_NUMBER = 1;
    private com.splicemachine.coprocessor.SpliceMessage.WriteResult.Code code_;
    /**
     * <code>optional .WriteResult.Code code = 1;</code>
     */
    public boolean hasCode() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional .WriteResult.Code code = 1;</code>
     */
    public com.splicemachine.coprocessor.SpliceMessage.WriteResult.Code getCode() {
      return code_;
    }

    // optional string errorMessage = 2;
    public static final int ERRORMESSAGE_FIELD_NUMBER = 2;
    private java.lang.Object errorMessage_;
    /**
     * <code>optional string errorMessage = 2;</code>
     */
    public boolean hasErrorMessage() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional string errorMessage = 2;</code>
     */
    public java.lang.String getErrorMessage() {
      java.lang.Object ref = errorMessage_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          errorMessage_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string errorMessage = 2;</code>
     */
    public com.google.protobuf.ByteString
        getErrorMessageBytes() {
      java.lang.Object ref = errorMessage_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        errorMessage_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // optional .ConstraintContext constraintContext = 3;
    public static final int CONSTRAINTCONTEXT_FIELD_NUMBER = 3;
    private com.splicemachine.coprocessor.SpliceMessage.ConstraintContext constraintContext_;
    /**
     * <code>optional .ConstraintContext constraintContext = 3;</code>
     */
    public boolean hasConstraintContext() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional .ConstraintContext constraintContext = 3;</code>
     */
    public com.splicemachine.coprocessor.SpliceMessage.ConstraintContext getConstraintContext() {
      return constraintContext_;
    }
    /**
     * <code>optional .ConstraintContext constraintContext = 3;</code>
     */
    public com.splicemachine.coprocessor.SpliceMessage.ConstraintContextOrBuilder getConstraintContextOrBuilder() {
      return constraintContext_;
    }

    private void initFields() {
      code_ = com.splicemachine.coprocessor.SpliceMessage.WriteResult.Code.FAILED;
      errorMessage_ = "";
      constraintContext_ = com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.getDefaultInstance();
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeEnum(1, code_.getNumber());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeBytes(2, getErrorMessageBytes());
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeMessage(3, constraintContext_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeEnumSize(1, code_.getNumber());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(2, getErrorMessageBytes());
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(3, constraintContext_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.splicemachine.coprocessor.SpliceMessage.WriteResult)) {
        return super.equals(obj);
      }
      com.splicemachine.coprocessor.SpliceMessage.WriteResult other = (com.splicemachine.coprocessor.SpliceMessage.WriteResult) obj;

      boolean result = true;
      result = result && (hasCode() == other.hasCode());
      if (hasCode()) {
        result = result &&
            (getCode() == other.getCode());
      }
      result = result && (hasErrorMessage() == other.hasErrorMessage());
      if (hasErrorMessage()) {
        result = result && getErrorMessage()
            .equals(other.getErrorMessage());
      }
      result = result && (hasConstraintContext() == other.hasConstraintContext());
      if (hasConstraintContext()) {
        result = result && getConstraintContext()
            .equals(other.getConstraintContext());
      }
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }

    private int memoizedHashCode = 0;
    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      if (hasCode()) {
        hash = (37 * hash) + CODE_FIELD_NUMBER;
        hash = (53 * hash) + hashEnum(getCode());
      }
      if (hasErrorMessage()) {
        hash = (37 * hash) + ERRORMESSAGE_FIELD_NUMBER;
        hash = (53 * hash) + getErrorMessage().hashCode();
      }
      if (hasConstraintContext()) {
        hash = (37 * hash) + CONSTRAINTCONTEXT_FIELD_NUMBER;
        hash = (53 * hash) + getConstraintContext().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.splicemachine.coprocessor.SpliceMessage.WriteResult parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.WriteResult parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.WriteResult parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.WriteResult parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.WriteResult parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.WriteResult parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.WriteResult parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.WriteResult parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.WriteResult parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.WriteResult parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.splicemachine.coprocessor.SpliceMessage.WriteResult prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code WriteResult}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.splicemachine.coprocessor.SpliceMessage.WriteResultOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_WriteResult_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_WriteResult_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.splicemachine.coprocessor.SpliceMessage.WriteResult.class, com.splicemachine.coprocessor.SpliceMessage.WriteResult.Builder.class);
      }

      // Construct using com.splicemachine.coprocessor.SpliceMessage.WriteResult.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
          getConstraintContextFieldBuilder();
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        code_ = com.splicemachine.coprocessor.SpliceMessage.WriteResult.Code.FAILED;
        bitField0_ = (bitField0_ & ~0x00000001);
        errorMessage_ = "";
        bitField0_ = (bitField0_ & ~0x00000002);
        if (constraintContextBuilder_ == null) {
          constraintContext_ = com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.getDefaultInstance();
        } else {
          constraintContextBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_WriteResult_descriptor;
      }

      public com.splicemachine.coprocessor.SpliceMessage.WriteResult getDefaultInstanceForType() {
        return com.splicemachine.coprocessor.SpliceMessage.WriteResult.getDefaultInstance();
      }

      public com.splicemachine.coprocessor.SpliceMessage.WriteResult build() {
        com.splicemachine.coprocessor.SpliceMessage.WriteResult result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.splicemachine.coprocessor.SpliceMessage.WriteResult buildPartial() {
        com.splicemachine.coprocessor.SpliceMessage.WriteResult result = new com.splicemachine.coprocessor.SpliceMessage.WriteResult(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.code_ = code_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.errorMessage_ = errorMessage_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        if (constraintContextBuilder_ == null) {
          result.constraintContext_ = constraintContext_;
        } else {
          result.constraintContext_ = constraintContextBuilder_.build();
        }
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.splicemachine.coprocessor.SpliceMessage.WriteResult) {
          return mergeFrom((com.splicemachine.coprocessor.SpliceMessage.WriteResult)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.splicemachine.coprocessor.SpliceMessage.WriteResult other) {
        if (other == com.splicemachine.coprocessor.SpliceMessage.WriteResult.getDefaultInstance()) return this;
        if (other.hasCode()) {
          setCode(other.getCode());
        }
        if (other.hasErrorMessage()) {
          bitField0_ |= 0x00000002;
          errorMessage_ = other.errorMessage_;
          onChanged();
        }
        if (other.hasConstraintContext()) {
          mergeConstraintContext(other.getConstraintContext());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.splicemachine.coprocessor.SpliceMessage.WriteResult parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.splicemachine.coprocessor.SpliceMessage.WriteResult) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // optional .WriteResult.Code code = 1;
      private com.splicemachine.coprocessor.SpliceMessage.WriteResult.Code code_ = com.splicemachine.coprocessor.SpliceMessage.WriteResult.Code.FAILED;
      /**
       * <code>optional .WriteResult.Code code = 1;</code>
       */
      public boolean hasCode() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional .WriteResult.Code code = 1;</code>
       */
      public com.splicemachine.coprocessor.SpliceMessage.WriteResult.Code getCode() {
        return code_;
      }
      /**
       * <code>optional .WriteResult.Code code = 1;</code>
       */
      public Builder setCode(com.splicemachine.coprocessor.SpliceMessage.WriteResult.Code value) {
        if (value == null) {
          throw new NullPointerException();
        }
        bitField0_ |= 0x00000001;
        code_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional .WriteResult.Code code = 1;</code>
       */
      public Builder clearCode() {
        bitField0_ = (bitField0_ & ~0x00000001);
        code_ = com.splicemachine.coprocessor.SpliceMessage.WriteResult.Code.FAILED;
        onChanged();
        return this;
      }

      // optional string errorMessage = 2;
      private java.lang.Object errorMessage_ = "";
      /**
       * <code>optional string errorMessage = 2;</code>
       */
      public boolean hasErrorMessage() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional string errorMessage = 2;</code>
       */
      public java.lang.String getErrorMessage() {
        java.lang.Object ref = errorMessage_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          errorMessage_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string errorMessage = 2;</code>
       */
      public com.google.protobuf.ByteString
          getErrorMessageBytes() {
        java.lang.Object ref = errorMessage_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          errorMessage_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string errorMessage = 2;</code>
       */
      public Builder setErrorMessage(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        errorMessage_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string errorMessage = 2;</code>
       */
      public Builder clearErrorMessage() {
        bitField0_ = (bitField0_ & ~0x00000002);
        errorMessage_ = getDefaultInstance().getErrorMessage();
        onChanged();
        return this;
      }
      /**
       * <code>optional string errorMessage = 2;</code>
       */
      public Builder setErrorMessageBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        errorMessage_ = value;
        onChanged();
        return this;
      }

      // optional .ConstraintContext constraintContext = 3;
      private com.splicemachine.coprocessor.SpliceMessage.ConstraintContext constraintContext_ = com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.getDefaultInstance();
      private com.google.protobuf.SingleFieldBuilder<
          com.splicemachine.coprocessor.SpliceMessage.ConstraintContext, com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.Builder, com.splicemachine.coprocessor.SpliceMessage.ConstraintContextOrBuilder> constraintContextBuilder_;
      /**
       * <code>optional .ConstraintContext constraintContext = 3;</code>
       */
      public boolean hasConstraintContext() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>optional .ConstraintContext constraintContext = 3;</code>
       */
      public com.splicemachine.coprocessor.SpliceMessage.ConstraintContext getConstraintContext() {
        if (constraintContextBuilder_ == null) {
          return constraintContext_;
        } else {
          return constraintContextBuilder_.getMessage();
        }
      }
      /**
       * <code>optional .ConstraintContext constraintContext = 3;</code>
       */
      public Builder setConstraintContext(com.splicemachine.coprocessor.SpliceMessage.ConstraintContext value) {
        if (constraintContextBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          constraintContext_ = value;
          onChanged();
        } else {
          constraintContextBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000004;
        return this;
      }
      /**
       * <code>optional .ConstraintContext constraintContext = 3;</code>
       */
      public Builder setConstraintContext(
          com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.Builder builderForValue) {
        if (constraintContextBuilder_ == null) {
          constraintContext_ = builderForValue.build();
          onChanged();
        } else {
          constraintContextBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000004;
        return this;
      }
      /**
       * <code>optional .ConstraintContext constraintContext = 3;</code>
       */
      public Builder mergeConstraintContext(com.splicemachine.coprocessor.SpliceMessage.ConstraintContext value) {
        if (constraintContextBuilder_ == null) {
          if (((bitField0_ & 0x00000004) == 0x00000004) &&
              constraintContext_ != com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.getDefaultInstance()) {
            constraintContext_ =
              com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.newBuilder(constraintContext_).mergeFrom(value).buildPartial();
          } else {
            constraintContext_ = value;
          }
          onChanged();
        } else {
          constraintContextBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000004;
        return this;
      }
      /**
       * <code>optional .ConstraintContext constraintContext = 3;</code>
       */
      public Builder clearConstraintContext() {
        if (constraintContextBuilder_ == null) {
          constraintContext_ = com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.getDefaultInstance();
          onChanged();
        } else {
          constraintContextBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }
      /**
       * <code>optional .ConstraintContext constraintContext = 3;</code>
       */
      public com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.Builder getConstraintContextBuilder() {
        bitField0_ |= 0x00000004;
        onChanged();
        return getConstraintContextFieldBuilder().getBuilder();
      }
      /**
       * <code>optional .ConstraintContext constraintContext = 3;</code>
       */
      public com.splicemachine.coprocessor.SpliceMessage.ConstraintContextOrBuilder getConstraintContextOrBuilder() {
        if (constraintContextBuilder_ != null) {
          return constraintContextBuilder_.getMessageOrBuilder();
        } else {
          return constraintContext_;
        }
      }
      /**
       * <code>optional .ConstraintContext constraintContext = 3;</code>
       */
      private com.google.protobuf.SingleFieldBuilder<
          com.splicemachine.coprocessor.SpliceMessage.ConstraintContext, com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.Builder, com.splicemachine.coprocessor.SpliceMessage.ConstraintContextOrBuilder> 
          getConstraintContextFieldBuilder() {
        if (constraintContextBuilder_ == null) {
          constraintContextBuilder_ = new com.google.protobuf.SingleFieldBuilder<
              com.splicemachine.coprocessor.SpliceMessage.ConstraintContext, com.splicemachine.coprocessor.SpliceMessage.ConstraintContext.Builder, com.splicemachine.coprocessor.SpliceMessage.ConstraintContextOrBuilder>(
                  constraintContext_,
                  getParentForChildren(),
                  isClean());
          constraintContext_ = null;
        }
        return constraintContextBuilder_;
      }

      // @@protoc_insertion_point(builder_scope:WriteResult)
    }

    static {
      defaultInstance = new WriteResult(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:WriteResult)
  }

  public interface BulkWriteResponseOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // optional bytes bytes = 1;
    /**
     * <code>optional bytes bytes = 1;</code>
     */
    boolean hasBytes();
    /**
     * <code>optional bytes bytes = 1;</code>
     */
    com.google.protobuf.ByteString getBytes();
  }
  /**
   * Protobuf type {@code BulkWriteResponse}
   */
  public static final class BulkWriteResponse extends
      com.google.protobuf.GeneratedMessage
      implements BulkWriteResponseOrBuilder {
    // Use BulkWriteResponse.newBuilder() to construct.
    private BulkWriteResponse(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private BulkWriteResponse(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final BulkWriteResponse defaultInstance;
    public static BulkWriteResponse getDefaultInstance() {
      return defaultInstance;
    }

    public BulkWriteResponse getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private BulkWriteResponse(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              bytes_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_BulkWriteResponse_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_BulkWriteResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse.class, com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse.Builder.class);
    }

    public static com.google.protobuf.Parser<BulkWriteResponse> PARSER =
        new com.google.protobuf.AbstractParser<BulkWriteResponse>() {
      public BulkWriteResponse parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new BulkWriteResponse(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<BulkWriteResponse> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // optional bytes bytes = 1;
    public static final int BYTES_FIELD_NUMBER = 1;
    private com.google.protobuf.ByteString bytes_;
    /**
     * <code>optional bytes bytes = 1;</code>
     */
    public boolean hasBytes() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional bytes bytes = 1;</code>
     */
    public com.google.protobuf.ByteString getBytes() {
      return bytes_;
    }

    private void initFields() {
      bytes_ = com.google.protobuf.ByteString.EMPTY;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, bytes_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, bytes_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse)) {
        return super.equals(obj);
      }
      com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse other = (com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse) obj;

      boolean result = true;
      result = result && (hasBytes() == other.hasBytes());
      if (hasBytes()) {
        result = result && getBytes()
            .equals(other.getBytes());
      }
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }

    private int memoizedHashCode = 0;
    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      if (hasBytes()) {
        hash = (37 * hash) + BYTES_FIELD_NUMBER;
        hash = (53 * hash) + getBytes().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code BulkWriteResponse}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_BulkWriteResponse_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_BulkWriteResponse_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse.class, com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse.Builder.class);
      }

      // Construct using com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        bytes_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_BulkWriteResponse_descriptor;
      }

      public com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse getDefaultInstanceForType() {
        return com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse.getDefaultInstance();
      }

      public com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse build() {
        com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse buildPartial() {
        com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse result = new com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.bytes_ = bytes_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse) {
          return mergeFrom((com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse other) {
        if (other == com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse.getDefaultInstance()) return this;
        if (other.hasBytes()) {
          setBytes(other.getBytes());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // optional bytes bytes = 1;
      private com.google.protobuf.ByteString bytes_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes bytes = 1;</code>
       */
      public boolean hasBytes() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional bytes bytes = 1;</code>
       */
      public com.google.protobuf.ByteString getBytes() {
        return bytes_;
      }
      /**
       * <code>optional bytes bytes = 1;</code>
       */
      public Builder setBytes(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        bytes_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes bytes = 1;</code>
       */
      public Builder clearBytes() {
        bitField0_ = (bitField0_ & ~0x00000001);
        bytes_ = getDefaultInstance().getBytes();
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:BulkWriteResponse)
    }

    static {
      defaultInstance = new BulkWriteResponse(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:BulkWriteResponse)
  }

  public interface BulkWriteRequestOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // optional bytes bytes = 1;
    /**
     * <code>optional bytes bytes = 1;</code>
     */
    boolean hasBytes();
    /**
     * <code>optional bytes bytes = 1;</code>
     */
    com.google.protobuf.ByteString getBytes();
  }
  /**
   * Protobuf type {@code BulkWriteRequest}
   */
  public static final class BulkWriteRequest extends
      com.google.protobuf.GeneratedMessage
      implements BulkWriteRequestOrBuilder {
    // Use BulkWriteRequest.newBuilder() to construct.
    private BulkWriteRequest(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private BulkWriteRequest(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final BulkWriteRequest defaultInstance;
    public static BulkWriteRequest getDefaultInstance() {
      return defaultInstance;
    }

    public BulkWriteRequest getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private BulkWriteRequest(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              bytes_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_BulkWriteRequest_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_BulkWriteRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest.class, com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest.Builder.class);
    }

    public static com.google.protobuf.Parser<BulkWriteRequest> PARSER =
        new com.google.protobuf.AbstractParser<BulkWriteRequest>() {
      public BulkWriteRequest parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new BulkWriteRequest(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<BulkWriteRequest> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // optional bytes bytes = 1;
    public static final int BYTES_FIELD_NUMBER = 1;
    private com.google.protobuf.ByteString bytes_;
    /**
     * <code>optional bytes bytes = 1;</code>
     */
    public boolean hasBytes() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional bytes bytes = 1;</code>
     */
    public com.google.protobuf.ByteString getBytes() {
      return bytes_;
    }

    private void initFields() {
      bytes_ = com.google.protobuf.ByteString.EMPTY;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, bytes_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, bytes_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest)) {
        return super.equals(obj);
      }
      com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest other = (com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest) obj;

      boolean result = true;
      result = result && (hasBytes() == other.hasBytes());
      if (hasBytes()) {
        result = result && getBytes()
            .equals(other.getBytes());
      }
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }

    private int memoizedHashCode = 0;
    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      if (hasBytes()) {
        hash = (37 * hash) + BYTES_FIELD_NUMBER;
        hash = (53 * hash) + getBytes().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code BulkWriteRequest}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_BulkWriteRequest_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_BulkWriteRequest_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest.class, com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest.Builder.class);
      }

      // Construct using com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        bytes_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_BulkWriteRequest_descriptor;
      }

      public com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest getDefaultInstanceForType() {
        return com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest.getDefaultInstance();
      }

      public com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest build() {
        com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest buildPartial() {
        com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest result = new com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.bytes_ = bytes_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest) {
          return mergeFrom((com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest other) {
        if (other == com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest.getDefaultInstance()) return this;
        if (other.hasBytes()) {
          setBytes(other.getBytes());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // optional bytes bytes = 1;
      private com.google.protobuf.ByteString bytes_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes bytes = 1;</code>
       */
      public boolean hasBytes() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional bytes bytes = 1;</code>
       */
      public com.google.protobuf.ByteString getBytes() {
        return bytes_;
      }
      /**
       * <code>optional bytes bytes = 1;</code>
       */
      public Builder setBytes(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        bytes_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes bytes = 1;</code>
       */
      public Builder clearBytes() {
        bitField0_ = (bitField0_ & ~0x00000001);
        bytes_ = getDefaultInstance().getBytes();
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:BulkWriteRequest)
    }

    static {
      defaultInstance = new BulkWriteRequest(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:BulkWriteRequest)
  }

  public interface DropIndexRequestOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // optional uint64 indexConglomId = 1;
    /**
     * <code>optional uint64 indexConglomId = 1;</code>
     */
    boolean hasIndexConglomId();
    /**
     * <code>optional uint64 indexConglomId = 1;</code>
     */
    long getIndexConglomId();

    // optional uint64 baseConglomId = 2;
    /**
     * <code>optional uint64 baseConglomId = 2;</code>
     */
    boolean hasBaseConglomId();
    /**
     * <code>optional uint64 baseConglomId = 2;</code>
     */
    long getBaseConglomId();

    // required int64 txnId = 3;
    /**
     * <code>required int64 txnId = 3;</code>
     */
    boolean hasTxnId();
    /**
     * <code>required int64 txnId = 3;</code>
     */
    long getTxnId();
  }
  /**
   * Protobuf type {@code DropIndexRequest}
   */
  public static final class DropIndexRequest extends
      com.google.protobuf.GeneratedMessage
      implements DropIndexRequestOrBuilder {
    // Use DropIndexRequest.newBuilder() to construct.
    private DropIndexRequest(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private DropIndexRequest(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final DropIndexRequest defaultInstance;
    public static DropIndexRequest getDefaultInstance() {
      return defaultInstance;
    }

    public DropIndexRequest getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private DropIndexRequest(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 8: {
              bitField0_ |= 0x00000001;
              indexConglomId_ = input.readUInt64();
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              baseConglomId_ = input.readUInt64();
              break;
            }
            case 24: {
              bitField0_ |= 0x00000004;
              txnId_ = input.readInt64();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_DropIndexRequest_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_DropIndexRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest.class, com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest.Builder.class);
    }

    public static com.google.protobuf.Parser<DropIndexRequest> PARSER =
        new com.google.protobuf.AbstractParser<DropIndexRequest>() {
      public DropIndexRequest parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new DropIndexRequest(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<DropIndexRequest> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // optional uint64 indexConglomId = 1;
    public static final int INDEXCONGLOMID_FIELD_NUMBER = 1;
    private long indexConglomId_;
    /**
     * <code>optional uint64 indexConglomId = 1;</code>
     */
    public boolean hasIndexConglomId() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional uint64 indexConglomId = 1;</code>
     */
    public long getIndexConglomId() {
      return indexConglomId_;
    }

    // optional uint64 baseConglomId = 2;
    public static final int BASECONGLOMID_FIELD_NUMBER = 2;
    private long baseConglomId_;
    /**
     * <code>optional uint64 baseConglomId = 2;</code>
     */
    public boolean hasBaseConglomId() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional uint64 baseConglomId = 2;</code>
     */
    public long getBaseConglomId() {
      return baseConglomId_;
    }

    // required int64 txnId = 3;
    public static final int TXNID_FIELD_NUMBER = 3;
    private long txnId_;
    /**
     * <code>required int64 txnId = 3;</code>
     */
    public boolean hasTxnId() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>required int64 txnId = 3;</code>
     */
    public long getTxnId() {
      return txnId_;
    }

    private void initFields() {
      indexConglomId_ = 0L;
      baseConglomId_ = 0L;
      txnId_ = 0L;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasTxnId()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeUInt64(1, indexConglomId_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeUInt64(2, baseConglomId_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeInt64(3, txnId_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeUInt64Size(1, indexConglomId_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeUInt64Size(2, baseConglomId_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(3, txnId_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest)) {
        return super.equals(obj);
      }
      com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest other = (com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest) obj;

      boolean result = true;
      result = result && (hasIndexConglomId() == other.hasIndexConglomId());
      if (hasIndexConglomId()) {
        result = result && (getIndexConglomId()
            == other.getIndexConglomId());
      }
      result = result && (hasBaseConglomId() == other.hasBaseConglomId());
      if (hasBaseConglomId()) {
        result = result && (getBaseConglomId()
            == other.getBaseConglomId());
      }
      result = result && (hasTxnId() == other.hasTxnId());
      if (hasTxnId()) {
        result = result && (getTxnId()
            == other.getTxnId());
      }
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }

    private int memoizedHashCode = 0;
    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      if (hasIndexConglomId()) {
        hash = (37 * hash) + INDEXCONGLOMID_FIELD_NUMBER;
        hash = (53 * hash) + hashLong(getIndexConglomId());
      }
      if (hasBaseConglomId()) {
        hash = (37 * hash) + BASECONGLOMID_FIELD_NUMBER;
        hash = (53 * hash) + hashLong(getBaseConglomId());
      }
      if (hasTxnId()) {
        hash = (37 * hash) + TXNID_FIELD_NUMBER;
        hash = (53 * hash) + hashLong(getTxnId());
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code DropIndexRequest}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.splicemachine.coprocessor.SpliceMessage.DropIndexRequestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_DropIndexRequest_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_DropIndexRequest_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest.class, com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest.Builder.class);
      }

      // Construct using com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        indexConglomId_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000001);
        baseConglomId_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000002);
        txnId_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_DropIndexRequest_descriptor;
      }

      public com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest getDefaultInstanceForType() {
        return com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest.getDefaultInstance();
      }

      public com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest build() {
        com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest buildPartial() {
        com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest result = new com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.indexConglomId_ = indexConglomId_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.baseConglomId_ = baseConglomId_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.txnId_ = txnId_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest) {
          return mergeFrom((com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest other) {
        if (other == com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest.getDefaultInstance()) return this;
        if (other.hasIndexConglomId()) {
          setIndexConglomId(other.getIndexConglomId());
        }
        if (other.hasBaseConglomId()) {
          setBaseConglomId(other.getBaseConglomId());
        }
        if (other.hasTxnId()) {
          setTxnId(other.getTxnId());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasTxnId()) {
          
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // optional uint64 indexConglomId = 1;
      private long indexConglomId_ ;
      /**
       * <code>optional uint64 indexConglomId = 1;</code>
       */
      public boolean hasIndexConglomId() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional uint64 indexConglomId = 1;</code>
       */
      public long getIndexConglomId() {
        return indexConglomId_;
      }
      /**
       * <code>optional uint64 indexConglomId = 1;</code>
       */
      public Builder setIndexConglomId(long value) {
        bitField0_ |= 0x00000001;
        indexConglomId_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional uint64 indexConglomId = 1;</code>
       */
      public Builder clearIndexConglomId() {
        bitField0_ = (bitField0_ & ~0x00000001);
        indexConglomId_ = 0L;
        onChanged();
        return this;
      }

      // optional uint64 baseConglomId = 2;
      private long baseConglomId_ ;
      /**
       * <code>optional uint64 baseConglomId = 2;</code>
       */
      public boolean hasBaseConglomId() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional uint64 baseConglomId = 2;</code>
       */
      public long getBaseConglomId() {
        return baseConglomId_;
      }
      /**
       * <code>optional uint64 baseConglomId = 2;</code>
       */
      public Builder setBaseConglomId(long value) {
        bitField0_ |= 0x00000002;
        baseConglomId_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional uint64 baseConglomId = 2;</code>
       */
      public Builder clearBaseConglomId() {
        bitField0_ = (bitField0_ & ~0x00000002);
        baseConglomId_ = 0L;
        onChanged();
        return this;
      }

      // required int64 txnId = 3;
      private long txnId_ ;
      /**
       * <code>required int64 txnId = 3;</code>
       */
      public boolean hasTxnId() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>required int64 txnId = 3;</code>
       */
      public long getTxnId() {
        return txnId_;
      }
      /**
       * <code>required int64 txnId = 3;</code>
       */
      public Builder setTxnId(long value) {
        bitField0_ |= 0x00000004;
        txnId_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required int64 txnId = 3;</code>
       */
      public Builder clearTxnId() {
        bitField0_ = (bitField0_ & ~0x00000004);
        txnId_ = 0L;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:DropIndexRequest)
    }

    static {
      defaultInstance = new DropIndexRequest(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:DropIndexRequest)
  }

  public interface DropIndexResponseOrBuilder
      extends com.google.protobuf.MessageOrBuilder {
  }
  /**
   * Protobuf type {@code DropIndexResponse}
   */
  public static final class DropIndexResponse extends
      com.google.protobuf.GeneratedMessage
      implements DropIndexResponseOrBuilder {
    // Use DropIndexResponse.newBuilder() to construct.
    private DropIndexResponse(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private DropIndexResponse(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final DropIndexResponse defaultInstance;
    public static DropIndexResponse getDefaultInstance() {
      return defaultInstance;
    }

    public DropIndexResponse getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private DropIndexResponse(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_DropIndexResponse_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.splicemachine.coprocessor.SpliceMessage.internal_static_DropIndexResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse.class, com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse.Builder.class);
    }

    public static com.google.protobuf.Parser<DropIndexResponse> PARSER =
        new com.google.protobuf.AbstractParser<DropIndexResponse>() {
      public DropIndexResponse parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new DropIndexResponse(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<DropIndexResponse> getParserForType() {
      return PARSER;
    }

    private void initFields() {
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse)) {
        return super.equals(obj);
      }
      com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse other = (com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse) obj;

      boolean result = true;
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }

    private int memoizedHashCode = 0;
    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code DropIndexResponse}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.splicemachine.coprocessor.SpliceMessage.DropIndexResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_DropIndexResponse_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_DropIndexResponse_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse.class, com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse.Builder.class);
      }

      // Construct using com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.splicemachine.coprocessor.SpliceMessage.internal_static_DropIndexResponse_descriptor;
      }

      public com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse getDefaultInstanceForType() {
        return com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse.getDefaultInstance();
      }

      public com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse build() {
        com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse buildPartial() {
        com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse result = new com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse(this);
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse) {
          return mergeFrom((com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse other) {
        if (other == com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse.getDefaultInstance()) return this;
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }

      // @@protoc_insertion_point(builder_scope:DropIndexResponse)
    }

    static {
      defaultInstance = new DropIndexResponse(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:DropIndexResponse)
  }

  /**
   * Protobuf service {@code SpliceIndexService}
   */
  public static abstract class SpliceIndexService
      implements com.google.protobuf.Service {
    protected SpliceIndexService() {}

    public interface Interface {
      /**
       * <code>rpc bulkWrite(.BulkWriteRequest) returns (.BulkWriteResponse);</code>
       */
      public abstract void bulkWrite(
          com.google.protobuf.RpcController controller,
          com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest request,
          com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse> done);

      /**
       * <code>rpc deleteFirstAfter(.DeleteFirstAfterRequest) returns (.WriteResult);</code>
       */
      public abstract void deleteFirstAfter(
          com.google.protobuf.RpcController controller,
          com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest request,
          com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.WriteResult> done);

    }

    public static com.google.protobuf.Service newReflectiveService(
        final Interface impl) {
      return new SpliceIndexService() {
        @java.lang.Override
        public  void bulkWrite(
            com.google.protobuf.RpcController controller,
            com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest request,
            com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse> done) {
          impl.bulkWrite(controller, request, done);
        }

        @java.lang.Override
        public  void deleteFirstAfter(
            com.google.protobuf.RpcController controller,
            com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest request,
            com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.WriteResult> done) {
          impl.deleteFirstAfter(controller, request, done);
        }

      };
    }

    public static com.google.protobuf.BlockingService
        newReflectiveBlockingService(final BlockingInterface impl) {
      return new com.google.protobuf.BlockingService() {
        public final com.google.protobuf.Descriptors.ServiceDescriptor
            getDescriptorForType() {
          return getDescriptor();
        }

        public final com.google.protobuf.Message callBlockingMethod(
            com.google.protobuf.Descriptors.MethodDescriptor method,
            com.google.protobuf.RpcController controller,
            com.google.protobuf.Message request)
            throws com.google.protobuf.ServiceException {
          if (method.getService() != getDescriptor()) {
            throw new java.lang.IllegalArgumentException(
              "Service.callBlockingMethod() given method descriptor for " +
              "wrong service type.");
          }
          switch(method.getIndex()) {
            case 0:
              return impl.bulkWrite(controller, (com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest)request);
            case 1:
              return impl.deleteFirstAfter(controller, (com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest)request);
            default:
              throw new java.lang.AssertionError("Can't get here.");
          }
        }

        public final com.google.protobuf.Message
            getRequestPrototype(
            com.google.protobuf.Descriptors.MethodDescriptor method) {
          if (method.getService() != getDescriptor()) {
            throw new java.lang.IllegalArgumentException(
              "Service.getRequestPrototype() given method " +
              "descriptor for wrong service type.");
          }
          switch(method.getIndex()) {
            case 0:
              return com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest.getDefaultInstance();
            case 1:
              return com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest.getDefaultInstance();
            default:
              throw new java.lang.AssertionError("Can't get here.");
          }
        }

        public final com.google.protobuf.Message
            getResponsePrototype(
            com.google.protobuf.Descriptors.MethodDescriptor method) {
          if (method.getService() != getDescriptor()) {
            throw new java.lang.IllegalArgumentException(
              "Service.getResponsePrototype() given method " +
              "descriptor for wrong service type.");
          }
          switch(method.getIndex()) {
            case 0:
              return com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse.getDefaultInstance();
            case 1:
              return com.splicemachine.coprocessor.SpliceMessage.WriteResult.getDefaultInstance();
            default:
              throw new java.lang.AssertionError("Can't get here.");
          }
        }

      };
    }

    /**
     * <code>rpc bulkWrite(.BulkWriteRequest) returns (.BulkWriteResponse);</code>
     */
    public abstract void bulkWrite(
        com.google.protobuf.RpcController controller,
        com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest request,
        com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse> done);

    /**
     * <code>rpc deleteFirstAfter(.DeleteFirstAfterRequest) returns (.WriteResult);</code>
     */
    public abstract void deleteFirstAfter(
        com.google.protobuf.RpcController controller,
        com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest request,
        com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.WriteResult> done);

    public static final
        com.google.protobuf.Descriptors.ServiceDescriptor
        getDescriptor() {
      return com.splicemachine.coprocessor.SpliceMessage.getDescriptor().getServices().get(0);
    }
    public final com.google.protobuf.Descriptors.ServiceDescriptor
        getDescriptorForType() {
      return getDescriptor();
    }

    public final void callMethod(
        com.google.protobuf.Descriptors.MethodDescriptor method,
        com.google.protobuf.RpcController controller,
        com.google.protobuf.Message request,
        com.google.protobuf.RpcCallback<
          com.google.protobuf.Message> done) {
      if (method.getService() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "Service.callMethod() given method descriptor for wrong " +
          "service type.");
      }
      switch(method.getIndex()) {
        case 0:
          this.bulkWrite(controller, (com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest)request,
            com.google.protobuf.RpcUtil.<com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse>specializeCallback(
              done));
          return;
        case 1:
          this.deleteFirstAfter(controller, (com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest)request,
            com.google.protobuf.RpcUtil.<com.splicemachine.coprocessor.SpliceMessage.WriteResult>specializeCallback(
              done));
          return;
        default:
          throw new java.lang.AssertionError("Can't get here.");
      }
    }

    public final com.google.protobuf.Message
        getRequestPrototype(
        com.google.protobuf.Descriptors.MethodDescriptor method) {
      if (method.getService() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "Service.getRequestPrototype() given method " +
          "descriptor for wrong service type.");
      }
      switch(method.getIndex()) {
        case 0:
          return com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest.getDefaultInstance();
        case 1:
          return com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest.getDefaultInstance();
        default:
          throw new java.lang.AssertionError("Can't get here.");
      }
    }

    public final com.google.protobuf.Message
        getResponsePrototype(
        com.google.protobuf.Descriptors.MethodDescriptor method) {
      if (method.getService() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "Service.getResponsePrototype() given method " +
          "descriptor for wrong service type.");
      }
      switch(method.getIndex()) {
        case 0:
          return com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse.getDefaultInstance();
        case 1:
          return com.splicemachine.coprocessor.SpliceMessage.WriteResult.getDefaultInstance();
        default:
          throw new java.lang.AssertionError("Can't get here.");
      }
    }

    public static Stub newStub(
        com.google.protobuf.RpcChannel channel) {
      return new Stub(channel);
    }

    public static final class Stub extends com.splicemachine.coprocessor.SpliceMessage.SpliceIndexService implements Interface {
      private Stub(com.google.protobuf.RpcChannel channel) {
        this.channel = channel;
      }

      private final com.google.protobuf.RpcChannel channel;

      public com.google.protobuf.RpcChannel getChannel() {
        return channel;
      }

      public  void bulkWrite(
          com.google.protobuf.RpcController controller,
          com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest request,
          com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse> done) {
        channel.callMethod(
          getDescriptor().getMethods().get(0),
          controller,
          request,
          com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse.getDefaultInstance(),
          com.google.protobuf.RpcUtil.generalizeCallback(
            done,
            com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse.class,
            com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse.getDefaultInstance()));
      }

      public  void deleteFirstAfter(
          com.google.protobuf.RpcController controller,
          com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest request,
          com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.WriteResult> done) {
        channel.callMethod(
          getDescriptor().getMethods().get(1),
          controller,
          request,
          com.splicemachine.coprocessor.SpliceMessage.WriteResult.getDefaultInstance(),
          com.google.protobuf.RpcUtil.generalizeCallback(
            done,
            com.splicemachine.coprocessor.SpliceMessage.WriteResult.class,
            com.splicemachine.coprocessor.SpliceMessage.WriteResult.getDefaultInstance()));
      }
    }

    public static BlockingInterface newBlockingStub(
        com.google.protobuf.BlockingRpcChannel channel) {
      return new BlockingStub(channel);
    }

    public interface BlockingInterface {
      public com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse bulkWrite(
          com.google.protobuf.RpcController controller,
          com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest request)
          throws com.google.protobuf.ServiceException;

      public com.splicemachine.coprocessor.SpliceMessage.WriteResult deleteFirstAfter(
          com.google.protobuf.RpcController controller,
          com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest request)
          throws com.google.protobuf.ServiceException;
    }

    private static final class BlockingStub implements BlockingInterface {
      private BlockingStub(com.google.protobuf.BlockingRpcChannel channel) {
        this.channel = channel;
      }

      private final com.google.protobuf.BlockingRpcChannel channel;

      public com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse bulkWrite(
          com.google.protobuf.RpcController controller,
          com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest request)
          throws com.google.protobuf.ServiceException {
        return (com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse) channel.callBlockingMethod(
          getDescriptor().getMethods().get(0),
          controller,
          request,
          com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse.getDefaultInstance());
      }


      public com.splicemachine.coprocessor.SpliceMessage.WriteResult deleteFirstAfter(
          com.google.protobuf.RpcController controller,
          com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest request)
          throws com.google.protobuf.ServiceException {
        return (com.splicemachine.coprocessor.SpliceMessage.WriteResult) channel.callBlockingMethod(
          getDescriptor().getMethods().get(1),
          controller,
          request,
          com.splicemachine.coprocessor.SpliceMessage.WriteResult.getDefaultInstance());
      }

    }

    // @@protoc_insertion_point(class_scope:SpliceIndexService)
  }

  /**
   * Protobuf service {@code SpliceIndexManagementService}
   */
  public static abstract class SpliceIndexManagementService
      implements com.google.protobuf.Service {
    protected SpliceIndexManagementService() {}

    public interface Interface {
      /**
       * <code>rpc dropIndex(.DropIndexRequest) returns (.DropIndexResponse);</code>
       */
      public abstract void dropIndex(
          com.google.protobuf.RpcController controller,
          com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest request,
          com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse> done);

    }

    public static com.google.protobuf.Service newReflectiveService(
        final Interface impl) {
      return new SpliceIndexManagementService() {
        @java.lang.Override
        public  void dropIndex(
            com.google.protobuf.RpcController controller,
            com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest request,
            com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse> done) {
          impl.dropIndex(controller, request, done);
        }

      };
    }

    public static com.google.protobuf.BlockingService
        newReflectiveBlockingService(final BlockingInterface impl) {
      return new com.google.protobuf.BlockingService() {
        public final com.google.protobuf.Descriptors.ServiceDescriptor
            getDescriptorForType() {
          return getDescriptor();
        }

        public final com.google.protobuf.Message callBlockingMethod(
            com.google.protobuf.Descriptors.MethodDescriptor method,
            com.google.protobuf.RpcController controller,
            com.google.protobuf.Message request)
            throws com.google.protobuf.ServiceException {
          if (method.getService() != getDescriptor()) {
            throw new java.lang.IllegalArgumentException(
              "Service.callBlockingMethod() given method descriptor for " +
              "wrong service type.");
          }
          switch(method.getIndex()) {
            case 0:
              return impl.dropIndex(controller, (com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest)request);
            default:
              throw new java.lang.AssertionError("Can't get here.");
          }
        }

        public final com.google.protobuf.Message
            getRequestPrototype(
            com.google.protobuf.Descriptors.MethodDescriptor method) {
          if (method.getService() != getDescriptor()) {
            throw new java.lang.IllegalArgumentException(
              "Service.getRequestPrototype() given method " +
              "descriptor for wrong service type.");
          }
          switch(method.getIndex()) {
            case 0:
              return com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest.getDefaultInstance();
            default:
              throw new java.lang.AssertionError("Can't get here.");
          }
        }

        public final com.google.protobuf.Message
            getResponsePrototype(
            com.google.protobuf.Descriptors.MethodDescriptor method) {
          if (method.getService() != getDescriptor()) {
            throw new java.lang.IllegalArgumentException(
              "Service.getResponsePrototype() given method " +
              "descriptor for wrong service type.");
          }
          switch(method.getIndex()) {
            case 0:
              return com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse.getDefaultInstance();
            default:
              throw new java.lang.AssertionError("Can't get here.");
          }
        }

      };
    }

    /**
     * <code>rpc dropIndex(.DropIndexRequest) returns (.DropIndexResponse);</code>
     */
    public abstract void dropIndex(
        com.google.protobuf.RpcController controller,
        com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest request,
        com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse> done);

    public static final
        com.google.protobuf.Descriptors.ServiceDescriptor
        getDescriptor() {
      return com.splicemachine.coprocessor.SpliceMessage.getDescriptor().getServices().get(1);
    }
    public final com.google.protobuf.Descriptors.ServiceDescriptor
        getDescriptorForType() {
      return getDescriptor();
    }

    public final void callMethod(
        com.google.protobuf.Descriptors.MethodDescriptor method,
        com.google.protobuf.RpcController controller,
        com.google.protobuf.Message request,
        com.google.protobuf.RpcCallback<
          com.google.protobuf.Message> done) {
      if (method.getService() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "Service.callMethod() given method descriptor for wrong " +
          "service type.");
      }
      switch(method.getIndex()) {
        case 0:
          this.dropIndex(controller, (com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest)request,
            com.google.protobuf.RpcUtil.<com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse>specializeCallback(
              done));
          return;
        default:
          throw new java.lang.AssertionError("Can't get here.");
      }
    }

    public final com.google.protobuf.Message
        getRequestPrototype(
        com.google.protobuf.Descriptors.MethodDescriptor method) {
      if (method.getService() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "Service.getRequestPrototype() given method " +
          "descriptor for wrong service type.");
      }
      switch(method.getIndex()) {
        case 0:
          return com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest.getDefaultInstance();
        default:
          throw new java.lang.AssertionError("Can't get here.");
      }
    }

    public final com.google.protobuf.Message
        getResponsePrototype(
        com.google.protobuf.Descriptors.MethodDescriptor method) {
      if (method.getService() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "Service.getResponsePrototype() given method " +
          "descriptor for wrong service type.");
      }
      switch(method.getIndex()) {
        case 0:
          return com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse.getDefaultInstance();
        default:
          throw new java.lang.AssertionError("Can't get here.");
      }
    }

    public static Stub newStub(
        com.google.protobuf.RpcChannel channel) {
      return new Stub(channel);
    }

    public static final class Stub extends com.splicemachine.coprocessor.SpliceMessage.SpliceIndexManagementService implements Interface {
      private Stub(com.google.protobuf.RpcChannel channel) {
        this.channel = channel;
      }

      private final com.google.protobuf.RpcChannel channel;

      public com.google.protobuf.RpcChannel getChannel() {
        return channel;
      }

      public  void dropIndex(
          com.google.protobuf.RpcController controller,
          com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest request,
          com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse> done) {
        channel.callMethod(
          getDescriptor().getMethods().get(0),
          controller,
          request,
          com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse.getDefaultInstance(),
          com.google.protobuf.RpcUtil.generalizeCallback(
            done,
            com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse.class,
            com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse.getDefaultInstance()));
      }
    }

    public static BlockingInterface newBlockingStub(
        com.google.protobuf.BlockingRpcChannel channel) {
      return new BlockingStub(channel);
    }

    public interface BlockingInterface {
      public com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse dropIndex(
          com.google.protobuf.RpcController controller,
          com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest request)
          throws com.google.protobuf.ServiceException;
    }

    private static final class BlockingStub implements BlockingInterface {
      private BlockingStub(com.google.protobuf.BlockingRpcChannel channel) {
        this.channel = channel;
      }

      private final com.google.protobuf.BlockingRpcChannel channel;

      public com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse dropIndex(
          com.google.protobuf.RpcController controller,
          com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest request)
          throws com.google.protobuf.ServiceException {
        return (com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse) channel.callBlockingMethod(
          getDescriptor().getMethods().get(0),
          controller,
          request,
          com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse.getDefaultInstance());
      }

    }

    // @@protoc_insertion_point(class_scope:SpliceIndexManagementService)
  }

  /**
   * Protobuf service {@code SpliceSchedulerService}
   */
  public static abstract class SpliceSchedulerService
      implements com.google.protobuf.Service {
    protected SpliceSchedulerService() {}

    public interface Interface {
      /**
       * <code>rpc submit(.SpliceSchedulerRequest) returns (.SchedulerResponse);</code>
       */
      public abstract void submit(
          com.google.protobuf.RpcController controller,
          com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest request,
          com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse> done);

    }

    public static com.google.protobuf.Service newReflectiveService(
        final Interface impl) {
      return new SpliceSchedulerService() {
        @java.lang.Override
        public  void submit(
            com.google.protobuf.RpcController controller,
            com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest request,
            com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse> done) {
          impl.submit(controller, request, done);
        }

      };
    }

    public static com.google.protobuf.BlockingService
        newReflectiveBlockingService(final BlockingInterface impl) {
      return new com.google.protobuf.BlockingService() {
        public final com.google.protobuf.Descriptors.ServiceDescriptor
            getDescriptorForType() {
          return getDescriptor();
        }

        public final com.google.protobuf.Message callBlockingMethod(
            com.google.protobuf.Descriptors.MethodDescriptor method,
            com.google.protobuf.RpcController controller,
            com.google.protobuf.Message request)
            throws com.google.protobuf.ServiceException {
          if (method.getService() != getDescriptor()) {
            throw new java.lang.IllegalArgumentException(
              "Service.callBlockingMethod() given method descriptor for " +
              "wrong service type.");
          }
          switch(method.getIndex()) {
            case 0:
              return impl.submit(controller, (com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest)request);
            default:
              throw new java.lang.AssertionError("Can't get here.");
          }
        }

        public final com.google.protobuf.Message
            getRequestPrototype(
            com.google.protobuf.Descriptors.MethodDescriptor method) {
          if (method.getService() != getDescriptor()) {
            throw new java.lang.IllegalArgumentException(
              "Service.getRequestPrototype() given method " +
              "descriptor for wrong service type.");
          }
          switch(method.getIndex()) {
            case 0:
              return com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest.getDefaultInstance();
            default:
              throw new java.lang.AssertionError("Can't get here.");
          }
        }

        public final com.google.protobuf.Message
            getResponsePrototype(
            com.google.protobuf.Descriptors.MethodDescriptor method) {
          if (method.getService() != getDescriptor()) {
            throw new java.lang.IllegalArgumentException(
              "Service.getResponsePrototype() given method " +
              "descriptor for wrong service type.");
          }
          switch(method.getIndex()) {
            case 0:
              return com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse.getDefaultInstance();
            default:
              throw new java.lang.AssertionError("Can't get here.");
          }
        }

      };
    }

    /**
     * <code>rpc submit(.SpliceSchedulerRequest) returns (.SchedulerResponse);</code>
     */
    public abstract void submit(
        com.google.protobuf.RpcController controller,
        com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest request,
        com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse> done);

    public static final
        com.google.protobuf.Descriptors.ServiceDescriptor
        getDescriptor() {
      return com.splicemachine.coprocessor.SpliceMessage.getDescriptor().getServices().get(2);
    }
    public final com.google.protobuf.Descriptors.ServiceDescriptor
        getDescriptorForType() {
      return getDescriptor();
    }

    public final void callMethod(
        com.google.protobuf.Descriptors.MethodDescriptor method,
        com.google.protobuf.RpcController controller,
        com.google.protobuf.Message request,
        com.google.protobuf.RpcCallback<
          com.google.protobuf.Message> done) {
      if (method.getService() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "Service.callMethod() given method descriptor for wrong " +
          "service type.");
      }
      switch(method.getIndex()) {
        case 0:
          this.submit(controller, (com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest)request,
            com.google.protobuf.RpcUtil.<com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse>specializeCallback(
              done));
          return;
        default:
          throw new java.lang.AssertionError("Can't get here.");
      }
    }

    public final com.google.protobuf.Message
        getRequestPrototype(
        com.google.protobuf.Descriptors.MethodDescriptor method) {
      if (method.getService() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "Service.getRequestPrototype() given method " +
          "descriptor for wrong service type.");
      }
      switch(method.getIndex()) {
        case 0:
          return com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest.getDefaultInstance();
        default:
          throw new java.lang.AssertionError("Can't get here.");
      }
    }

    public final com.google.protobuf.Message
        getResponsePrototype(
        com.google.protobuf.Descriptors.MethodDescriptor method) {
      if (method.getService() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "Service.getResponsePrototype() given method " +
          "descriptor for wrong service type.");
      }
      switch(method.getIndex()) {
        case 0:
          return com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse.getDefaultInstance();
        default:
          throw new java.lang.AssertionError("Can't get here.");
      }
    }

    public static Stub newStub(
        com.google.protobuf.RpcChannel channel) {
      return new Stub(channel);
    }

    public static final class Stub extends com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerService implements Interface {
      private Stub(com.google.protobuf.RpcChannel channel) {
        this.channel = channel;
      }

      private final com.google.protobuf.RpcChannel channel;

      public com.google.protobuf.RpcChannel getChannel() {
        return channel;
      }

      public  void submit(
          com.google.protobuf.RpcController controller,
          com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest request,
          com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse> done) {
        channel.callMethod(
          getDescriptor().getMethods().get(0),
          controller,
          request,
          com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse.getDefaultInstance(),
          com.google.protobuf.RpcUtil.generalizeCallback(
            done,
            com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse.class,
            com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse.getDefaultInstance()));
      }
    }

    public static BlockingInterface newBlockingStub(
        com.google.protobuf.BlockingRpcChannel channel) {
      return new BlockingStub(channel);
    }

    public interface BlockingInterface {
      public com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse submit(
          com.google.protobuf.RpcController controller,
          com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest request)
          throws com.google.protobuf.ServiceException;
    }

    private static final class BlockingStub implements BlockingInterface {
      private BlockingStub(com.google.protobuf.BlockingRpcChannel channel) {
        this.channel = channel;
      }

      private final com.google.protobuf.BlockingRpcChannel channel;

      public com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse submit(
          com.google.protobuf.RpcController controller,
          com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest request)
          throws com.google.protobuf.ServiceException {
        return (com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse) channel.callBlockingMethod(
          getDescriptor().getMethods().get(0),
          controller,
          request,
          com.splicemachine.coprocessor.SpliceMessage.SchedulerResponse.getDefaultInstance());
      }

    }

    // @@protoc_insertion_point(class_scope:SpliceSchedulerService)
  }

  /**
   * Protobuf service {@code SpliceDerbyCoprocessorService}
   */
  public static abstract class SpliceDerbyCoprocessorService
      implements com.google.protobuf.Service {
    protected SpliceDerbyCoprocessorService() {}

    public interface Interface {
    }

    public static com.google.protobuf.Service newReflectiveService(
        final Interface impl) {
      return new SpliceDerbyCoprocessorService() {
      };
    }

    public static com.google.protobuf.BlockingService
        newReflectiveBlockingService(final BlockingInterface impl) {
      return new com.google.protobuf.BlockingService() {
        public final com.google.protobuf.Descriptors.ServiceDescriptor
            getDescriptorForType() {
          return getDescriptor();
        }

        public final com.google.protobuf.Message callBlockingMethod(
            com.google.protobuf.Descriptors.MethodDescriptor method,
            com.google.protobuf.RpcController controller,
            com.google.protobuf.Message request)
            throws com.google.protobuf.ServiceException {
          if (method.getService() != getDescriptor()) {
            throw new java.lang.IllegalArgumentException(
              "Service.callBlockingMethod() given method descriptor for " +
              "wrong service type.");
          }
          switch(method.getIndex()) {
            default:
              throw new java.lang.AssertionError("Can't get here.");
          }
        }

        public final com.google.protobuf.Message
            getRequestPrototype(
            com.google.protobuf.Descriptors.MethodDescriptor method) {
          if (method.getService() != getDescriptor()) {
            throw new java.lang.IllegalArgumentException(
              "Service.getRequestPrototype() given method " +
              "descriptor for wrong service type.");
          }
          switch(method.getIndex()) {
            default:
              throw new java.lang.AssertionError("Can't get here.");
          }
        }

        public final com.google.protobuf.Message
            getResponsePrototype(
            com.google.protobuf.Descriptors.MethodDescriptor method) {
          if (method.getService() != getDescriptor()) {
            throw new java.lang.IllegalArgumentException(
              "Service.getResponsePrototype() given method " +
              "descriptor for wrong service type.");
          }
          switch(method.getIndex()) {
            default:
              throw new java.lang.AssertionError("Can't get here.");
          }
        }

      };
    }

    public static final
        com.google.protobuf.Descriptors.ServiceDescriptor
        getDescriptor() {
      return com.splicemachine.coprocessor.SpliceMessage.getDescriptor().getServices().get(3);
    }
    public final com.google.protobuf.Descriptors.ServiceDescriptor
        getDescriptorForType() {
      return getDescriptor();
    }

    public final void callMethod(
        com.google.protobuf.Descriptors.MethodDescriptor method,
        com.google.protobuf.RpcController controller,
        com.google.protobuf.Message request,
        com.google.protobuf.RpcCallback<
          com.google.protobuf.Message> done) {
      if (method.getService() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "Service.callMethod() given method descriptor for wrong " +
          "service type.");
      }
      switch(method.getIndex()) {
        default:
          throw new java.lang.AssertionError("Can't get here.");
      }
    }

    public final com.google.protobuf.Message
        getRequestPrototype(
        com.google.protobuf.Descriptors.MethodDescriptor method) {
      if (method.getService() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "Service.getRequestPrototype() given method " +
          "descriptor for wrong service type.");
      }
      switch(method.getIndex()) {
        default:
          throw new java.lang.AssertionError("Can't get here.");
      }
    }

    public final com.google.protobuf.Message
        getResponsePrototype(
        com.google.protobuf.Descriptors.MethodDescriptor method) {
      if (method.getService() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "Service.getResponsePrototype() given method " +
          "descriptor for wrong service type.");
      }
      switch(method.getIndex()) {
        default:
          throw new java.lang.AssertionError("Can't get here.");
      }
    }

    public static Stub newStub(
        com.google.protobuf.RpcChannel channel) {
      return new Stub(channel);
    }

    public static final class Stub extends com.splicemachine.coprocessor.SpliceMessage.SpliceDerbyCoprocessorService implements Interface {
      private Stub(com.google.protobuf.RpcChannel channel) {
        this.channel = channel;
      }

      private final com.google.protobuf.RpcChannel channel;

      public com.google.protobuf.RpcChannel getChannel() {
        return channel;
      }
    }

    public static BlockingInterface newBlockingStub(
        com.google.protobuf.BlockingRpcChannel channel) {
      return new BlockingStub(channel);
    }

    public interface BlockingInterface {}

    private static final class BlockingStub implements BlockingInterface {
      private BlockingStub(com.google.protobuf.BlockingRpcChannel channel) {
        this.channel = channel;
      }

      private final com.google.protobuf.BlockingRpcChannel channel;
    }

    // @@protoc_insertion_point(class_scope:SpliceDerbyCoprocessorService)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_SpliceSchedulerRequest_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_SpliceSchedulerRequest_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_TaskFutureResponse_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_TaskFutureResponse_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_SchedulerResponse_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_SchedulerResponse_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_DeleteFirstAfterRequest_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_DeleteFirstAfterRequest_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_ConstraintContext_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_ConstraintContext_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_AllocateFilterMessage_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_AllocateFilterMessage_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_SuccessFilterMessage_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_SuccessFilterMessage_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_SkippingScanFilterMessage_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_SkippingScanFilterMessage_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_HbaseAttributeHolderMessage_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_HbaseAttributeHolderMessage_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_HbaseAttributeHolderMessage_Attribute_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_HbaseAttributeHolderMessage_Attribute_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_WriteResult_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_WriteResult_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_BulkWriteResponse_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_BulkWriteResponse_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_BulkWriteRequest_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_BulkWriteRequest_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_DropIndexRequest_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_DropIndexRequest_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_DropIndexResponse_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_DropIndexResponse_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\014Splice.proto\032\014Client.proto\"x\n\026SpliceSc" +
      "hedulerRequest\022\021\n\ttaskStart\030\001 \001(\014\022\017\n\007tas" +
      "kEnd\030\002 \001(\014\022\021\n\tclassName\030\003 \001(\t\022\022\n\nclassBy" +
      "tes\030\004 \001(\014\022\023\n\013allowSplits\030\005 \001(\010\"_\n\022TaskFu" +
      "tureResponse\022\020\n\010taskNode\030\001 \002(\t\022\016\n\006taskId" +
      "\030\002 \002(\014\022\025\n\restimatedCost\030\003 \001(\001\022\020\n\010startRo" +
      "w\030\004 \002(\014\":\n\021SchedulerResponse\022%\n\010response" +
      "\030\001 \003(\0132\023.TaskFutureResponse\"O\n\027DeleteFir" +
      "stAfterRequest\022\025\n\rtransactionId\030\001 \001(\t\022\016\n" +
      "\006rowKey\030\002 \001(\014\022\r\n\005limit\030\003 \001(\014\">\n\021Constrai",
      "ntContext\022\021\n\ttableName\030\001 \001(\t\022\026\n\016constrai" +
      "ntName\030\002 \001(\t\"-\n\025AllocateFilterMessage\022\024\n" +
      "\014addressMatch\030\001 \001(\014\"+\n\024SuccessFilterMess" +
      "age\022\023\n\013failedTasks\030\001 \003(\014\"T\n\031SkippingScan" +
      "FilterMessage\022\021\n\tstartKeys\030\001 \003(\014\022\020\n\010stop" +
      "Keys\030\002 \003(\014\022\022\n\npredicates\030\003 \003(\014\"\203\001\n\033Hbase" +
      "AttributeHolderMessage\022:\n\nattributes\030\001 \003" +
      "(\0132&.HbaseAttributeHolderMessage.Attribu" +
      "te\032(\n\tAttribute\022\014\n\004name\030\001 \002(\t\022\r\n\005value\030\002" +
      " \002(\014\"\344\002\n\013WriteResult\022\037\n\004code\030\001 \001(\0162\021.Wri",
      "teResult.Code\022\024\n\014errorMessage\030\002 \001(\t\022-\n\021c" +
      "onstraintContext\030\003 \001(\0132\022.ConstraintConte" +
      "xt\"\356\001\n\004Code\022\n\n\006FAILED\020\000\022\022\n\016WRITE_CONFLIC" +
      "T\020\001\022\013\n\007SUCCESS\020\002\022\031\n\025PRIMARY_KEY_VIOLATIO" +
      "N\020\003\022\024\n\020UNIQUE_VIOLATION\020\004\022\031\n\025FOREIGN_KEY" +
      "_VIOLATION\020\005\022\023\n\017CHECK_VIOLATION\020\006\022\026\n\022NOT" +
      "_SERVING_REGION\020\007\022\020\n\014WRONG_REGION\020\010\022\023\n\017R" +
      "EGION_TOO_BUSY\020\t\022\013\n\007NOT_RUN\020\n\022\014\n\010NOT_NUL" +
      "L\020\013\"\"\n\021BulkWriteResponse\022\r\n\005bytes\030\001 \001(\014\"" +
      "!\n\020BulkWriteRequest\022\r\n\005bytes\030\001 \001(\014\"P\n\020Dr",
      "opIndexRequest\022\026\n\016indexConglomId\030\001 \001(\004\022\025" +
      "\n\rbaseConglomId\030\002 \001(\004\022\r\n\005txnId\030\003 \002(\003\"\023\n\021" +
      "DropIndexResponse2\204\001\n\022SpliceIndexService" +
      "\0222\n\tbulkWrite\022\021.BulkWriteRequest\032\022.BulkW" +
      "riteResponse\022:\n\020deleteFirstAfter\022\030.Delet" +
      "eFirstAfterRequest\032\014.WriteResult2R\n\034Spli" +
      "ceIndexManagementService\0222\n\tdropIndex\022\021." +
      "DropIndexRequest\032\022.DropIndexResponse2O\n\026" +
      "SpliceSchedulerService\0225\n\006submit\022\027.Splic" +
      "eSchedulerRequest\032\022.SchedulerResponse2\037\n",
      "\035SpliceDerbyCoprocessorServiceB6\n\035com.sp" +
      "licemachine.coprocessorB\rSpliceMessageH\001" +
      "\210\001\001\240\001\001"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_SpliceSchedulerRequest_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_SpliceSchedulerRequest_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_SpliceSchedulerRequest_descriptor,
              new java.lang.String[] { "TaskStart", "TaskEnd", "ClassName", "ClassBytes", "AllowSplits", });
          internal_static_TaskFutureResponse_descriptor =
            getDescriptor().getMessageTypes().get(1);
          internal_static_TaskFutureResponse_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_TaskFutureResponse_descriptor,
              new java.lang.String[] { "TaskNode", "TaskId", "EstimatedCost", "StartRow", });
          internal_static_SchedulerResponse_descriptor =
            getDescriptor().getMessageTypes().get(2);
          internal_static_SchedulerResponse_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_SchedulerResponse_descriptor,
              new java.lang.String[] { "Response", });
          internal_static_DeleteFirstAfterRequest_descriptor =
            getDescriptor().getMessageTypes().get(3);
          internal_static_DeleteFirstAfterRequest_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_DeleteFirstAfterRequest_descriptor,
              new java.lang.String[] { "TransactionId", "RowKey", "Limit", });
          internal_static_ConstraintContext_descriptor =
            getDescriptor().getMessageTypes().get(4);
          internal_static_ConstraintContext_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_ConstraintContext_descriptor,
              new java.lang.String[] { "TableName", "ConstraintName", });
          internal_static_AllocateFilterMessage_descriptor =
            getDescriptor().getMessageTypes().get(5);
          internal_static_AllocateFilterMessage_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_AllocateFilterMessage_descriptor,
              new java.lang.String[] { "AddressMatch", });
          internal_static_SuccessFilterMessage_descriptor =
            getDescriptor().getMessageTypes().get(6);
          internal_static_SuccessFilterMessage_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_SuccessFilterMessage_descriptor,
              new java.lang.String[] { "FailedTasks", });
          internal_static_SkippingScanFilterMessage_descriptor =
            getDescriptor().getMessageTypes().get(7);
          internal_static_SkippingScanFilterMessage_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_SkippingScanFilterMessage_descriptor,
              new java.lang.String[] { "StartKeys", "StopKeys", "Predicates", });
          internal_static_HbaseAttributeHolderMessage_descriptor =
            getDescriptor().getMessageTypes().get(8);
          internal_static_HbaseAttributeHolderMessage_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_HbaseAttributeHolderMessage_descriptor,
              new java.lang.String[] { "Attributes", });
          internal_static_HbaseAttributeHolderMessage_Attribute_descriptor =
            internal_static_HbaseAttributeHolderMessage_descriptor.getNestedTypes().get(0);
          internal_static_HbaseAttributeHolderMessage_Attribute_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_HbaseAttributeHolderMessage_Attribute_descriptor,
              new java.lang.String[] { "Name", "Value", });
          internal_static_WriteResult_descriptor =
            getDescriptor().getMessageTypes().get(9);
          internal_static_WriteResult_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_WriteResult_descriptor,
              new java.lang.String[] { "Code", "ErrorMessage", "ConstraintContext", });
          internal_static_BulkWriteResponse_descriptor =
            getDescriptor().getMessageTypes().get(10);
          internal_static_BulkWriteResponse_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_BulkWriteResponse_descriptor,
              new java.lang.String[] { "Bytes", });
          internal_static_BulkWriteRequest_descriptor =
            getDescriptor().getMessageTypes().get(11);
          internal_static_BulkWriteRequest_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_BulkWriteRequest_descriptor,
              new java.lang.String[] { "Bytes", });
          internal_static_DropIndexRequest_descriptor =
            getDescriptor().getMessageTypes().get(12);
          internal_static_DropIndexRequest_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_DropIndexRequest_descriptor,
              new java.lang.String[] { "IndexConglomId", "BaseConglomId", "TxnId", });
          internal_static_DropIndexResponse_descriptor =
            getDescriptor().getMessageTypes().get(13);
          internal_static_DropIndexResponse_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_DropIndexResponse_descriptor,
              new java.lang.String[] { });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          org.apache.hadoop.hbase.protobuf.generated.ClientProtos.getDescriptor(),
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
