// Code generated by protoc-gen-go. DO NOT EDIT.
// source: hardware_service.proto

package polarbox_hardware_service

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import timestamp "github.com/golang/protobuf/ptypes/timestamp"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type RetCode int32

const (
	RetCode_OK   RetCode = 0
	RetCode_FAIL RetCode = 1
)

var RetCode_name = map[int32]string{
	0: "OK",
	1: "FAIL",
}

var RetCode_value = map[string]int32{
	"OK":   0,
	"FAIL": 1,
}

func (x RetCode) String() string {
	return proto.EnumName(RetCode_name, int32(x))
}

func (RetCode) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_073bce33c2e23a20, []int{0}
}

type RequestInfo struct {
	Appname              string   `protobuf:"bytes,1,opt,name=appname,proto3" json:"appname,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RequestInfo) Reset()         { *m = RequestInfo{} }
func (m *RequestInfo) String() string { return proto.CompactTextString(m) }
func (*RequestInfo) ProtoMessage()    {}
func (*RequestInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_073bce33c2e23a20, []int{0}
}
func (m *RequestInfo) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RequestInfo.Unmarshal(m, b)
}
func (m *RequestInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RequestInfo.Marshal(b, m, deterministic)
}
func (dst *RequestInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RequestInfo.Merge(dst, src)
}
func (m *RequestInfo) XXX_Size() int {
	return xxx_messageInfo_RequestInfo.Size(m)
}
func (m *RequestInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_RequestInfo.DiscardUnknown(m)
}

var xxx_messageInfo_RequestInfo proto.InternalMessageInfo

func (m *RequestInfo) GetAppname() string {
	if m != nil {
		return m.Appname
	}
	return ""
}

type ResponseInfo struct {
	Ret                  RetCode  `protobuf:"varint,1,opt,name=ret,proto3,enum=polarbox.hardware_service.RetCode" json:"ret,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ResponseInfo) Reset()         { *m = ResponseInfo{} }
func (m *ResponseInfo) String() string { return proto.CompactTextString(m) }
func (*ResponseInfo) ProtoMessage()    {}
func (*ResponseInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_073bce33c2e23a20, []int{1}
}
func (m *ResponseInfo) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ResponseInfo.Unmarshal(m, b)
}
func (m *ResponseInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ResponseInfo.Marshal(b, m, deterministic)
}
func (dst *ResponseInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ResponseInfo.Merge(dst, src)
}
func (m *ResponseInfo) XXX_Size() int {
	return xxx_messageInfo_ResponseInfo.Size(m)
}
func (m *ResponseInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_ResponseInfo.DiscardUnknown(m)
}

var xxx_messageInfo_ResponseInfo proto.InternalMessageInfo

func (m *ResponseInfo) GetRet() RetCode {
	if m != nil {
		return m.Ret
	}
	return RetCode_OK
}

type GetDBServerStatusRequest struct {
	Info                 *RequestInfo `protobuf:"bytes,1,opt,name=info,proto3" json:"info,omitempty"`
	Hostip               string       `protobuf:"bytes,2,opt,name=hostip,proto3" json:"hostip,omitempty"`
	Hostname             string       `protobuf:"bytes,3,opt,name=hostname,proto3" json:"hostname,omitempty"`
	XXX_NoUnkeyedLiteral struct{}     `json:"-"`
	XXX_unrecognized     []byte       `json:"-"`
	XXX_sizecache        int32        `json:"-"`
}

func (m *GetDBServerStatusRequest) Reset()         { *m = GetDBServerStatusRequest{} }
func (m *GetDBServerStatusRequest) String() string { return proto.CompactTextString(m) }
func (*GetDBServerStatusRequest) ProtoMessage()    {}
func (*GetDBServerStatusRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_073bce33c2e23a20, []int{2}
}
func (m *GetDBServerStatusRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetDBServerStatusRequest.Unmarshal(m, b)
}
func (m *GetDBServerStatusRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetDBServerStatusRequest.Marshal(b, m, deterministic)
}
func (dst *GetDBServerStatusRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetDBServerStatusRequest.Merge(dst, src)
}
func (m *GetDBServerStatusRequest) XXX_Size() int {
	return xxx_messageInfo_GetDBServerStatusRequest.Size(m)
}
func (m *GetDBServerStatusRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_GetDBServerStatusRequest.DiscardUnknown(m)
}

var xxx_messageInfo_GetDBServerStatusRequest proto.InternalMessageInfo

func (m *GetDBServerStatusRequest) GetInfo() *RequestInfo {
	if m != nil {
		return m.Info
	}
	return nil
}

func (m *GetDBServerStatusRequest) GetHostip() string {
	if m != nil {
		return m.Hostip
	}
	return ""
}

func (m *GetDBServerStatusRequest) GetHostname() string {
	if m != nil {
		return m.Hostname
	}
	return ""
}

type GetDBServerStatusResponse struct {
	Info                 *ResponseInfo        `protobuf:"bytes,1,opt,name=info,proto3" json:"info,omitempty"`
	Status               string               `protobuf:"bytes,2,opt,name=status,proto3" json:"status,omitempty"`
	Timestamp            *timestamp.Timestamp `protobuf:"bytes,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	XXX_NoUnkeyedLiteral struct{}             `json:"-"`
	XXX_unrecognized     []byte               `json:"-"`
	XXX_sizecache        int32                `json:"-"`
}

func (m *GetDBServerStatusResponse) Reset()         { *m = GetDBServerStatusResponse{} }
func (m *GetDBServerStatusResponse) String() string { return proto.CompactTextString(m) }
func (*GetDBServerStatusResponse) ProtoMessage()    {}
func (*GetDBServerStatusResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_073bce33c2e23a20, []int{3}
}
func (m *GetDBServerStatusResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetDBServerStatusResponse.Unmarshal(m, b)
}
func (m *GetDBServerStatusResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetDBServerStatusResponse.Marshal(b, m, deterministic)
}
func (dst *GetDBServerStatusResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetDBServerStatusResponse.Merge(dst, src)
}
func (m *GetDBServerStatusResponse) XXX_Size() int {
	return xxx_messageInfo_GetDBServerStatusResponse.Size(m)
}
func (m *GetDBServerStatusResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_GetDBServerStatusResponse.DiscardUnknown(m)
}

var xxx_messageInfo_GetDBServerStatusResponse proto.InternalMessageInfo

func (m *GetDBServerStatusResponse) GetInfo() *ResponseInfo {
	if m != nil {
		return m.Info
	}
	return nil
}

func (m *GetDBServerStatusResponse) GetStatus() string {
	if m != nil {
		return m.Status
	}
	return ""
}

func (m *GetDBServerStatusResponse) GetTimestamp() *timestamp.Timestamp {
	if m != nil {
		return m.Timestamp
	}
	return nil
}

type GetSwitchStatusRequest struct {
	Info                 *RequestInfo `protobuf:"bytes,1,opt,name=info,proto3" json:"info,omitempty"`
	Hostip               string       `protobuf:"bytes,2,opt,name=hostip,proto3" json:"hostip,omitempty"`
	Hostname             string       `protobuf:"bytes,3,opt,name=hostname,proto3" json:"hostname,omitempty"`
	XXX_NoUnkeyedLiteral struct{}     `json:"-"`
	XXX_unrecognized     []byte       `json:"-"`
	XXX_sizecache        int32        `json:"-"`
}

func (m *GetSwitchStatusRequest) Reset()         { *m = GetSwitchStatusRequest{} }
func (m *GetSwitchStatusRequest) String() string { return proto.CompactTextString(m) }
func (*GetSwitchStatusRequest) ProtoMessage()    {}
func (*GetSwitchStatusRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_073bce33c2e23a20, []int{4}
}
func (m *GetSwitchStatusRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetSwitchStatusRequest.Unmarshal(m, b)
}
func (m *GetSwitchStatusRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetSwitchStatusRequest.Marshal(b, m, deterministic)
}
func (dst *GetSwitchStatusRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetSwitchStatusRequest.Merge(dst, src)
}
func (m *GetSwitchStatusRequest) XXX_Size() int {
	return xxx_messageInfo_GetSwitchStatusRequest.Size(m)
}
func (m *GetSwitchStatusRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_GetSwitchStatusRequest.DiscardUnknown(m)
}

var xxx_messageInfo_GetSwitchStatusRequest proto.InternalMessageInfo

func (m *GetSwitchStatusRequest) GetInfo() *RequestInfo {
	if m != nil {
		return m.Info
	}
	return nil
}

func (m *GetSwitchStatusRequest) GetHostip() string {
	if m != nil {
		return m.Hostip
	}
	return ""
}

func (m *GetSwitchStatusRequest) GetHostname() string {
	if m != nil {
		return m.Hostname
	}
	return ""
}

type PortStatus struct {
	OperStatus           int32    `protobuf:"varint,1,opt,name=OperStatus,proto3" json:"OperStatus,omitempty"`
	AdminStatus          int32    `protobuf:"varint,2,opt,name=AdminStatus,proto3" json:"AdminStatus,omitempty"`
	Name                 string   `protobuf:"bytes,3,opt,name=Name,proto3" json:"Name,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PortStatus) Reset()         { *m = PortStatus{} }
func (m *PortStatus) String() string { return proto.CompactTextString(m) }
func (*PortStatus) ProtoMessage()    {}
func (*PortStatus) Descriptor() ([]byte, []int) {
	return fileDescriptor_073bce33c2e23a20, []int{5}
}
func (m *PortStatus) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PortStatus.Unmarshal(m, b)
}
func (m *PortStatus) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PortStatus.Marshal(b, m, deterministic)
}
func (dst *PortStatus) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PortStatus.Merge(dst, src)
}
func (m *PortStatus) XXX_Size() int {
	return xxx_messageInfo_PortStatus.Size(m)
}
func (m *PortStatus) XXX_DiscardUnknown() {
	xxx_messageInfo_PortStatus.DiscardUnknown(m)
}

var xxx_messageInfo_PortStatus proto.InternalMessageInfo

func (m *PortStatus) GetOperStatus() int32 {
	if m != nil {
		return m.OperStatus
	}
	return 0
}

func (m *PortStatus) GetAdminStatus() int32 {
	if m != nil {
		return m.AdminStatus
	}
	return 0
}

func (m *PortStatus) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

type GetSwitchStatusResponse struct {
	Info                 *ResponseInfo        `protobuf:"bytes,1,opt,name=info,proto3" json:"info,omitempty"`
	Status               []*PortStatus        `protobuf:"bytes,2,rep,name=status,proto3" json:"status,omitempty"`
	Timestamp            *timestamp.Timestamp `protobuf:"bytes,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	XXX_NoUnkeyedLiteral struct{}             `json:"-"`
	XXX_unrecognized     []byte               `json:"-"`
	XXX_sizecache        int32                `json:"-"`
}

func (m *GetSwitchStatusResponse) Reset()         { *m = GetSwitchStatusResponse{} }
func (m *GetSwitchStatusResponse) String() string { return proto.CompactTextString(m) }
func (*GetSwitchStatusResponse) ProtoMessage()    {}
func (*GetSwitchStatusResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_073bce33c2e23a20, []int{6}
}
func (m *GetSwitchStatusResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetSwitchStatusResponse.Unmarshal(m, b)
}
func (m *GetSwitchStatusResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetSwitchStatusResponse.Marshal(b, m, deterministic)
}
func (dst *GetSwitchStatusResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetSwitchStatusResponse.Merge(dst, src)
}
func (m *GetSwitchStatusResponse) XXX_Size() int {
	return xxx_messageInfo_GetSwitchStatusResponse.Size(m)
}
func (m *GetSwitchStatusResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_GetSwitchStatusResponse.DiscardUnknown(m)
}

var xxx_messageInfo_GetSwitchStatusResponse proto.InternalMessageInfo

func (m *GetSwitchStatusResponse) GetInfo() *ResponseInfo {
	if m != nil {
		return m.Info
	}
	return nil
}

func (m *GetSwitchStatusResponse) GetStatus() []*PortStatus {
	if m != nil {
		return m.Status
	}
	return nil
}

func (m *GetSwitchStatusResponse) GetTimestamp() *timestamp.Timestamp {
	if m != nil {
		return m.Timestamp
	}
	return nil
}

func init() {
	proto.RegisterType((*RequestInfo)(nil), "polarbox.hardware_service.RequestInfo")
	proto.RegisterType((*ResponseInfo)(nil), "polarbox.hardware_service.ResponseInfo")
	proto.RegisterType((*GetDBServerStatusRequest)(nil), "polarbox.hardware_service.GetDBServerStatusRequest")
	proto.RegisterType((*GetDBServerStatusResponse)(nil), "polarbox.hardware_service.GetDBServerStatusResponse")
	proto.RegisterType((*GetSwitchStatusRequest)(nil), "polarbox.hardware_service.GetSwitchStatusRequest")
	proto.RegisterType((*PortStatus)(nil), "polarbox.hardware_service.PortStatus")
	proto.RegisterType((*GetSwitchStatusResponse)(nil), "polarbox.hardware_service.GetSwitchStatusResponse")
	proto.RegisterEnum("polarbox.hardware_service.RetCode", RetCode_name, RetCode_value)
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// HardwareClient is the client API for Hardware service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type HardwareClient interface {
	GetDBServerStatus(ctx context.Context, in *GetDBServerStatusRequest, opts ...grpc.CallOption) (*GetDBServerStatusResponse, error)
	GetSwitchStatus(ctx context.Context, in *GetSwitchStatusRequest, opts ...grpc.CallOption) (*GetSwitchStatusResponse, error)
}

type hardwareClient struct {
	cc *grpc.ClientConn
}

func NewHardwareClient(cc *grpc.ClientConn) HardwareClient {
	return &hardwareClient{cc}
}

func (c *hardwareClient) GetDBServerStatus(ctx context.Context, in *GetDBServerStatusRequest, opts ...grpc.CallOption) (*GetDBServerStatusResponse, error) {
	out := new(GetDBServerStatusResponse)
	err := c.cc.Invoke(ctx, "/polarbox.hardware_service.Hardware/GetDBServerStatus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *hardwareClient) GetSwitchStatus(ctx context.Context, in *GetSwitchStatusRequest, opts ...grpc.CallOption) (*GetSwitchStatusResponse, error) {
	out := new(GetSwitchStatusResponse)
	err := c.cc.Invoke(ctx, "/polarbox.hardware_service.Hardware/GetSwitchStatus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// HardwareServer is the server API for Hardware service.
type HardwareServer interface {
	GetDBServerStatus(context.Context, *GetDBServerStatusRequest) (*GetDBServerStatusResponse, error)
	GetSwitchStatus(context.Context, *GetSwitchStatusRequest) (*GetSwitchStatusResponse, error)
}

func RegisterHardwareServer(s *grpc.Server, srv HardwareServer) {
	s.RegisterService(&_Hardware_serviceDesc, srv)
}

func _Hardware_GetDBServerStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetDBServerStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(HardwareServer).GetDBServerStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/polarbox.hardware_service.Hardware/GetDBServerStatus",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(HardwareServer).GetDBServerStatus(ctx, req.(*GetDBServerStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Hardware_GetSwitchStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetSwitchStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(HardwareServer).GetSwitchStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/polarbox.hardware_service.Hardware/GetSwitchStatus",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(HardwareServer).GetSwitchStatus(ctx, req.(*GetSwitchStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Hardware_serviceDesc = grpc.ServiceDesc{
	ServiceName: "polarbox.hardware_service.Hardware",
	HandlerType: (*HardwareServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetDBServerStatus",
			Handler:    _Hardware_GetDBServerStatus_Handler,
		},
		{
			MethodName: "GetSwitchStatus",
			Handler:    _Hardware_GetSwitchStatus_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "hardware_service.proto",
}

func init() { proto.RegisterFile("hardware_service.proto", fileDescriptor_073bce33c2e23a20) }

var fileDescriptor_073bce33c2e23a20 = []byte{
	// 447 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xc4, 0x93, 0xcf, 0x6e, 0xd3, 0x40,
	0x10, 0xc6, 0x71, 0x92, 0xa6, 0xe9, 0x18, 0x41, 0x99, 0x43, 0x70, 0x8d, 0x04, 0x95, 0x25, 0x68,
	0xc5, 0xc1, 0x15, 0x6e, 0x0f, 0x08, 0xc4, 0xa1, 0x50, 0x51, 0x2a, 0x10, 0x45, 0x6b, 0xee, 0xc8,
	0x6e, 0x26, 0x8d, 0xa5, 0xda, 0xbb, 0xec, 0x6e, 0x9a, 0x9c, 0x38, 0x71, 0x40, 0x3c, 0x0a, 0x2f,
	0xc3, 0x2b, 0xa1, 0xac, 0xed, 0x38, 0xe4, 0x8f, 0xa5, 0x08, 0xa4, 0xde, 0x3c, 0xeb, 0x6f, 0x66,
	0x7e, 0xf3, 0xcd, 0x2e, 0x74, 0x07, 0x91, 0xec, 0x8d, 0x22, 0x49, 0x5f, 0x14, 0xc9, 0xeb, 0xe4,
	0x82, 0x7c, 0x21, 0xb9, 0xe6, 0xb8, 0x23, 0xf8, 0x55, 0x24, 0x63, 0x3e, 0xf6, 0xe7, 0x05, 0xee,
	0xa3, 0x4b, 0xce, 0x2f, 0xaf, 0xe8, 0xc0, 0x08, 0xe3, 0x61, 0xff, 0x40, 0x27, 0x29, 0x29, 0x1d,
	0xa5, 0x22, 0xcf, 0xf5, 0xf6, 0xc0, 0x66, 0xf4, 0x75, 0x48, 0x4a, 0x9f, 0x65, 0x7d, 0x8e, 0x0e,
	0x6c, 0x46, 0x42, 0x64, 0x51, 0x4a, 0x8e, 0xb5, 0x6b, 0xed, 0x6f, 0xb1, 0x32, 0xf4, 0x4e, 0xe0,
	0x36, 0x23, 0x25, 0x78, 0xa6, 0xc8, 0x28, 0x8f, 0xa0, 0x29, 0x49, 0x1b, 0xd5, 0x9d, 0xc0, 0xf3,
	0x57, 0x22, 0xf8, 0x8c, 0xf4, 0x1b, 0xde, 0x23, 0x36, 0x91, 0x7b, 0x3f, 0x2d, 0x70, 0x4e, 0x49,
	0x9f, 0xbc, 0x0e, 0x49, 0x5e, 0x93, 0x0c, 0x75, 0xa4, 0x87, 0xaa, 0x00, 0xc0, 0x17, 0xd0, 0x4a,
	0xb2, 0x3e, 0x37, 0x35, 0xed, 0xe0, 0x49, 0x6d, 0xcd, 0x29, 0x32, 0x33, 0x39, 0xd8, 0x85, 0xf6,
	0x80, 0x2b, 0x9d, 0x08, 0xa7, 0x61, 0xb8, 0x8b, 0x08, 0x5d, 0xe8, 0x4c, 0xbe, 0xcc, 0x44, 0x4d,
	0xf3, 0x67, 0x1a, 0x7b, 0xbf, 0x2c, 0xd8, 0x59, 0x02, 0x93, 0x0f, 0x89, 0x2f, 0xff, 0xa2, 0xd9,
	0xab, 0xa5, 0xa9, 0x7c, 0xa9, 0x70, 0x94, 0x29, 0x57, 0xe2, 0xe4, 0x11, 0x3e, 0x87, 0xad, 0xe9,
	0x06, 0x0c, 0x8f, 0x1d, 0xb8, 0x7e, 0xbe, 0x23, 0xbf, 0xdc, 0x91, 0xff, 0xb9, 0x54, 0xb0, 0x4a,
	0xec, 0xfd, 0xb0, 0xa0, 0x7b, 0x4a, 0x3a, 0x1c, 0x25, 0xfa, 0x62, 0x70, 0xb3, 0xbe, 0xc5, 0x00,
	0x9f, 0xb8, 0xd4, 0x39, 0x04, 0x3e, 0x04, 0x38, 0x17, 0xa5, 0x7b, 0x86, 0x61, 0x83, 0xcd, 0x9c,
	0xe0, 0x2e, 0xd8, 0xc7, 0xbd, 0x34, 0xc9, 0xc2, 0xca, 0x8f, 0x0d, 0x36, 0x7b, 0x84, 0x08, 0xad,
	0x8f, 0x55, 0x1f, 0xf3, 0xed, 0xfd, 0xb6, 0xe0, 0xfe, 0xc2, 0xb8, 0xff, 0x63, 0x33, 0xaf, 0x66,
	0x36, 0xd3, 0xdc, 0xb7, 0x83, 0xc7, 0x35, 0xe9, 0xd5, 0x94, 0xff, 0xbe, 0xc0, 0xa7, 0x0f, 0x60,
	0xb3, 0x78, 0x0a, 0xd8, 0x86, 0xc6, 0xf9, 0xfb, 0xed, 0x5b, 0xd8, 0x81, 0xd6, 0xdb, 0xe3, 0xb3,
	0x0f, 0xdb, 0x56, 0xf0, 0xbd, 0x01, 0x9d, 0x77, 0x45, 0x7b, 0xfc, 0x06, 0xf7, 0x16, 0xae, 0x25,
	0x1e, 0xd6, 0x70, 0xae, 0x7a, 0x51, 0xee, 0xd1, 0x7a, 0x49, 0x85, 0xbf, 0x63, 0xb8, 0x3b, 0x67,
	0x3d, 0x3e, 0xab, 0x2f, 0xb4, 0xe4, 0x56, 0xba, 0xc1, 0x3a, 0x29, 0x79, 0xe7, 0xb8, 0x6d, 0x2c,
	0x3c, 0xfc, 0x13, 0x00, 0x00, 0xff, 0xff, 0xfc, 0x95, 0x91, 0x09, 0xea, 0x04, 0x00, 0x00,
}
