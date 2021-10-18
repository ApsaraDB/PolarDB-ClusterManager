// Code generated by protoc-gen-go. DO NOT EDIT.
// source: cluster.proto

package smartclient_service

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

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

// define the data type of request
type Request struct {
	Username             string   `protobuf:"bytes,1,opt,name=username,proto3" json:"username,omitempty"`
	Password             string   `protobuf:"bytes,2,opt,name=password,proto3" json:"password,omitempty"`
	Database             string   `protobuf:"bytes,3,opt,name=database,proto3" json:"database,omitempty"`
	Message              string   `protobuf:"bytes,4,opt,name=message,proto3" json:"message,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Request) Reset()         { *m = Request{} }
func (m *Request) String() string { return proto.CompactTextString(m) }
func (*Request) ProtoMessage()    {}
func (*Request) Descriptor() ([]byte, []int) {
	return fileDescriptor_3cfb3b8ec240c376, []int{0}
}
func (m *Request) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Request.Unmarshal(m, b)
}
func (m *Request) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Request.Marshal(b, m, deterministic)
}
func (dst *Request) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Request.Merge(dst, src)
}
func (m *Request) XXX_Size() int {
	return xxx_messageInfo_Request.Size(m)
}
func (m *Request) XXX_DiscardUnknown() {
	xxx_messageInfo_Request.DiscardUnknown(m)
}

var xxx_messageInfo_Request proto.InternalMessageInfo

func (m *Request) GetUsername() string {
	if m != nil {
		return m.Username
	}
	return ""
}

func (m *Request) GetPassword() string {
	if m != nil {
		return m.Password
	}
	return ""
}

func (m *Request) GetDatabase() string {
	if m != nil {
		return m.Database
	}
	return ""
}

func (m *Request) GetMessage() string {
	if m != nil {
		return m.Message
	}
	return ""
}

// define the data type of response
type Response struct {
	Message              string         `protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`
	TopologyEvent        *TopologyEvent `protobuf:"bytes,2,opt,name=topology_event,json=topologyEvent,proto3" json:"topology_event,omitempty"`
	HaEvent              *HAEvent       `protobuf:"bytes,3,opt,name=ha_event,json=haEvent,proto3" json:"ha_event,omitempty"`
	LbEvent              *LBEvent       `protobuf:"bytes,4,opt,name=lb_event,json=lbEvent,proto3" json:"lb_event,omitempty"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *Response) Reset()         { *m = Response{} }
func (m *Response) String() string { return proto.CompactTextString(m) }
func (*Response) ProtoMessage()    {}
func (*Response) Descriptor() ([]byte, []int) {
	return fileDescriptor_3cfb3b8ec240c376, []int{1}
}
func (m *Response) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Response.Unmarshal(m, b)
}
func (m *Response) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Response.Marshal(b, m, deterministic)
}
func (dst *Response) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Response.Merge(dst, src)
}
func (m *Response) XXX_Size() int {
	return xxx_messageInfo_Response.Size(m)
}
func (m *Response) XXX_DiscardUnknown() {
	xxx_messageInfo_Response.DiscardUnknown(m)
}

var xxx_messageInfo_Response proto.InternalMessageInfo

func (m *Response) GetMessage() string {
	if m != nil {
		return m.Message
	}
	return ""
}

func (m *Response) GetTopologyEvent() *TopologyEvent {
	if m != nil {
		return m.TopologyEvent
	}
	return nil
}

func (m *Response) GetHaEvent() *HAEvent {
	if m != nil {
		return m.HaEvent
	}
	return nil
}

func (m *Response) GetLbEvent() *LBEvent {
	if m != nil {
		return m.LbEvent
	}
	return nil
}

type TopologyEvent struct {
	Topology             []*TopologyNode `protobuf:"bytes,1,rep,name=topology,proto3" json:"topology,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *TopologyEvent) Reset()         { *m = TopologyEvent{} }
func (m *TopologyEvent) String() string { return proto.CompactTextString(m) }
func (*TopologyEvent) ProtoMessage()    {}
func (*TopologyEvent) Descriptor() ([]byte, []int) {
	return fileDescriptor_3cfb3b8ec240c376, []int{2}
}
func (m *TopologyEvent) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_TopologyEvent.Unmarshal(m, b)
}
func (m *TopologyEvent) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_TopologyEvent.Marshal(b, m, deterministic)
}
func (dst *TopologyEvent) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TopologyEvent.Merge(dst, src)
}
func (m *TopologyEvent) XXX_Size() int {
	return xxx_messageInfo_TopologyEvent.Size(m)
}
func (m *TopologyEvent) XXX_DiscardUnknown() {
	xxx_messageInfo_TopologyEvent.DiscardUnknown(m)
}

var xxx_messageInfo_TopologyEvent proto.InternalMessageInfo

func (m *TopologyEvent) GetTopology() []*TopologyNode {
	if m != nil {
		return m.Topology
	}
	return nil
}

type TopologyNode struct {
	Maxscale             []*MaxScale `protobuf:"bytes,1,rep,name=maxscale,proto3" json:"maxscale,omitempty"`
	Rw                   []*DBNode   `protobuf:"bytes,2,rep,name=rw,proto3" json:"rw,omitempty"`
	Ro                   []*DBNode   `protobuf:"bytes,3,rep,name=ro,proto3" json:"ro,omitempty"`
	Cm                   []*CMNode   `protobuf:"bytes,4,rep,name=cm,proto3" json:"cm,omitempty"`
	IsMaster             bool        `protobuf:"varint,5,opt,name=is_master,json=isMaster,proto3" json:"is_master,omitempty"`
	IsDataMax            bool        `protobuf:"varint,6,opt,name=is_datamax,json=isDataMax,proto3" json:"is_datamax,omitempty"`
	XXX_NoUnkeyedLiteral struct{}    `json:"-"`
	XXX_unrecognized     []byte      `json:"-"`
	XXX_sizecache        int32       `json:"-"`
}

func (m *TopologyNode) Reset()         { *m = TopologyNode{} }
func (m *TopologyNode) String() string { return proto.CompactTextString(m) }
func (*TopologyNode) ProtoMessage()    {}
func (*TopologyNode) Descriptor() ([]byte, []int) {
	return fileDescriptor_3cfb3b8ec240c376, []int{3}
}
func (m *TopologyNode) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_TopologyNode.Unmarshal(m, b)
}
func (m *TopologyNode) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_TopologyNode.Marshal(b, m, deterministic)
}
func (dst *TopologyNode) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TopologyNode.Merge(dst, src)
}
func (m *TopologyNode) XXX_Size() int {
	return xxx_messageInfo_TopologyNode.Size(m)
}
func (m *TopologyNode) XXX_DiscardUnknown() {
	xxx_messageInfo_TopologyNode.DiscardUnknown(m)
}

var xxx_messageInfo_TopologyNode proto.InternalMessageInfo

func (m *TopologyNode) GetMaxscale() []*MaxScale {
	if m != nil {
		return m.Maxscale
	}
	return nil
}

func (m *TopologyNode) GetRw() []*DBNode {
	if m != nil {
		return m.Rw
	}
	return nil
}

func (m *TopologyNode) GetRo() []*DBNode {
	if m != nil {
		return m.Ro
	}
	return nil
}

func (m *TopologyNode) GetCm() []*CMNode {
	if m != nil {
		return m.Cm
	}
	return nil
}

func (m *TopologyNode) GetIsMaster() bool {
	if m != nil {
		return m.IsMaster
	}
	return false
}

type CMNode struct {
	Region               string   `protobuf:"bytes,1,opt,name=region,proto3" json:"region,omitempty"`
	Az                   string   `protobuf:"bytes,2,opt,name=az,proto3" json:"az,omitempty"`
	Endpoint             string   `protobuf:"bytes,3,opt,name=endpoint,proto3" json:"endpoint,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *CMNode) Reset()         { *m = CMNode{} }
func (m *CMNode) String() string { return proto.CompactTextString(m) }
func (*CMNode) ProtoMessage()    {}
func (*CMNode) Descriptor() ([]byte, []int) {
	return fileDescriptor_3cfb3b8ec240c376, []int{4}
}
func (m *CMNode) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CMNode.Unmarshal(m, b)
}
func (m *CMNode) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CMNode.Marshal(b, m, deterministic)
}
func (dst *CMNode) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CMNode.Merge(dst, src)
}
func (m *CMNode) XXX_Size() int {
	return xxx_messageInfo_CMNode.Size(m)
}
func (m *CMNode) XXX_DiscardUnknown() {
	xxx_messageInfo_CMNode.DiscardUnknown(m)
}

var xxx_messageInfo_CMNode proto.InternalMessageInfo

func (m *CMNode) GetRegion() string {
	if m != nil {
		return m.Region
	}
	return ""
}

func (m *CMNode) GetAz() string {
	if m != nil {
		return m.Az
	}
	return ""
}

func (m *CMNode) GetEndpoint() string {
	if m != nil {
		return m.Endpoint
	}
	return ""
}

type MaxScale struct {
	Region               string   `protobuf:"bytes,1,opt,name=region,proto3" json:"region,omitempty"`
	Az                   string   `protobuf:"bytes,2,opt,name=az,proto3" json:"az,omitempty"`
	Endpoint             string   `protobuf:"bytes,3,opt,name=endpoint,proto3" json:"endpoint,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MaxScale) Reset()         { *m = MaxScale{} }
func (m *MaxScale) String() string { return proto.CompactTextString(m) }
func (*MaxScale) ProtoMessage()    {}
func (*MaxScale) Descriptor() ([]byte, []int) {
	return fileDescriptor_3cfb3b8ec240c376, []int{5}
}
func (m *MaxScale) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MaxScale.Unmarshal(m, b)
}
func (m *MaxScale) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MaxScale.Marshal(b, m, deterministic)
}
func (dst *MaxScale) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MaxScale.Merge(dst, src)
}
func (m *MaxScale) XXX_Size() int {
	return xxx_messageInfo_MaxScale.Size(m)
}
func (m *MaxScale) XXX_DiscardUnknown() {
	xxx_messageInfo_MaxScale.DiscardUnknown(m)
}

var xxx_messageInfo_MaxScale proto.InternalMessageInfo

func (m *MaxScale) GetRegion() string {
	if m != nil {
		return m.Region
	}
	return ""
}

func (m *MaxScale) GetAz() string {
	if m != nil {
		return m.Az
	}
	return ""
}

func (m *MaxScale) GetEndpoint() string {
	if m != nil {
		return m.Endpoint
	}
	return ""
}

type DBNode struct {
	Region               string   `protobuf:"bytes,1,opt,name=region,proto3" json:"region,omitempty"`
	Az                   string   `protobuf:"bytes,2,opt,name=az,proto3" json:"az,omitempty"`
	Endpoint             string   `protobuf:"bytes,3,opt,name=endpoint,proto3" json:"endpoint,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *DBNode) Reset()         { *m = DBNode{} }
func (m *DBNode) String() string { return proto.CompactTextString(m) }
func (*DBNode) ProtoMessage()    {}
func (*DBNode) Descriptor() ([]byte, []int) {
	return fileDescriptor_3cfb3b8ec240c376, []int{6}
}
func (m *DBNode) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_DBNode.Unmarshal(m, b)
}
func (m *DBNode) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_DBNode.Marshal(b, m, deterministic)
}
func (dst *DBNode) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DBNode.Merge(dst, src)
}
func (m *DBNode) XXX_Size() int {
	return xxx_messageInfo_DBNode.Size(m)
}
func (m *DBNode) XXX_DiscardUnknown() {
	xxx_messageInfo_DBNode.DiscardUnknown(m)
}

var xxx_messageInfo_DBNode proto.InternalMessageInfo

func (m *DBNode) GetRegion() string {
	if m != nil {
		return m.Region
	}
	return ""
}

func (m *DBNode) GetAz() string {
	if m != nil {
		return m.Az
	}
	return ""
}

func (m *DBNode) GetEndpoint() string {
	if m != nil {
		return m.Endpoint
	}
	return ""
}

type HAEvent struct {
	EventType            string   `protobuf:"bytes,1,opt,name=event_type,json=eventType,proto3" json:"event_type,omitempty"`
	Host                 string   `protobuf:"bytes,2,opt,name=host,proto3" json:"host,omitempty"`
	Status               string   `protobuf:"bytes,3,opt,name=status,proto3" json:"status,omitempty"`
	Reason               string   `protobuf:"bytes,4,opt,name=reason,proto3" json:"reason,omitempty"`
	Timestamp            string   `protobuf:"bytes,5,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *HAEvent) Reset()         { *m = HAEvent{} }
func (m *HAEvent) String() string { return proto.CompactTextString(m) }
func (*HAEvent) ProtoMessage()    {}
func (*HAEvent) Descriptor() ([]byte, []int) {
	return fileDescriptor_3cfb3b8ec240c376, []int{7}
}
func (m *HAEvent) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_HAEvent.Unmarshal(m, b)
}
func (m *HAEvent) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_HAEvent.Marshal(b, m, deterministic)
}
func (dst *HAEvent) XXX_Merge(src proto.Message) {
	xxx_messageInfo_HAEvent.Merge(dst, src)
}
func (m *HAEvent) XXX_Size() int {
	return xxx_messageInfo_HAEvent.Size(m)
}
func (m *HAEvent) XXX_DiscardUnknown() {
	xxx_messageInfo_HAEvent.DiscardUnknown(m)
}

var xxx_messageInfo_HAEvent proto.InternalMessageInfo

func (m *HAEvent) GetEventType() string {
	if m != nil {
		return m.EventType
	}
	return ""
}

func (m *HAEvent) GetHost() string {
	if m != nil {
		return m.Host
	}
	return ""
}

func (m *HAEvent) GetStatus() string {
	if m != nil {
		return m.Status
	}
	return ""
}

func (m *HAEvent) GetReason() string {
	if m != nil {
		return m.Reason
	}
	return ""
}

func (m *HAEvent) GetTimestamp() string {
	if m != nil {
		return m.Timestamp
	}
	return ""
}

type LBEvent struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *LBEvent) Reset()         { *m = LBEvent{} }
func (m *LBEvent) String() string { return proto.CompactTextString(m) }
func (*LBEvent) ProtoMessage()    {}
func (*LBEvent) Descriptor() ([]byte, []int) {
	return fileDescriptor_3cfb3b8ec240c376, []int{8}
}
func (m *LBEvent) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_LBEvent.Unmarshal(m, b)
}
func (m *LBEvent) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_LBEvent.Marshal(b, m, deterministic)
}
func (dst *LBEvent) XXX_Merge(src proto.Message) {
	xxx_messageInfo_LBEvent.Merge(dst, src)
}
func (m *LBEvent) XXX_Size() int {
	return xxx_messageInfo_LBEvent.Size(m)
}
func (m *LBEvent) XXX_DiscardUnknown() {
	xxx_messageInfo_LBEvent.DiscardUnknown(m)
}

var xxx_messageInfo_LBEvent proto.InternalMessageInfo

func init() {
	proto.RegisterType((*Request)(nil), "smartclient_service.Request")
	proto.RegisterType((*Response)(nil), "smartclient_service.Response")
	proto.RegisterType((*TopologyEvent)(nil), "smartclient_service.TopologyEvent")
	proto.RegisterType((*TopologyNode)(nil), "smartclient_service.TopologyNode")
	proto.RegisterType((*CMNode)(nil), "smartclient_service.CMNode")
	proto.RegisterType((*MaxScale)(nil), "smartclient_service.MaxScale")
	proto.RegisterType((*DBNode)(nil), "smartclient_service.DBNode")
	proto.RegisterType((*HAEvent)(nil), "smartclient_service.HAEvent")
	proto.RegisterType((*LBEvent)(nil), "smartclient_service.LBEvent")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// PolarDBManagerServiceClient is the client API for PolarDBManagerService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type PolarDBManagerServiceClient interface {
	// define the interface and data type
	SubscribeEvent(ctx context.Context, opts ...grpc.CallOption) (PolarDBManagerService_SubscribeEventClient, error)
}

type polarDBManagerServiceClient struct {
	cc *grpc.ClientConn
}

func NewPolarDBManagerServiceClient(cc *grpc.ClientConn) PolarDBManagerServiceClient {
	return &polarDBManagerServiceClient{cc}
}

func (c *polarDBManagerServiceClient) SubscribeEvent(ctx context.Context, opts ...grpc.CallOption) (PolarDBManagerService_SubscribeEventClient, error) {
	stream, err := c.cc.NewStream(ctx, &_PolarDBManagerService_serviceDesc.Streams[0], "/smartclient_service.PolarDBManagerService/SubscribeEvent", opts...)
	if err != nil {
		return nil, err
	}
	x := &polarDBManagerServiceSubscribeEventClient{stream}
	return x, nil
}

type PolarDBManagerService_SubscribeEventClient interface {
	Send(*Request) error
	Recv() (*Response, error)
	grpc.ClientStream
}

type polarDBManagerServiceSubscribeEventClient struct {
	grpc.ClientStream
}

func (x *polarDBManagerServiceSubscribeEventClient) Send(m *Request) error {
	return x.ClientStream.SendMsg(m)
}

func (x *polarDBManagerServiceSubscribeEventClient) Recv() (*Response, error) {
	m := new(Response)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// PolarDBManagerServiceServer is the server API for PolarDBManagerService service.
type PolarDBManagerServiceServer interface {
	// define the interface and data type
	SubscribeEvent(PolarDBManagerService_SubscribeEventServer) error
}

func RegisterPolarDBManagerServiceServer(s *grpc.Server, srv PolarDBManagerServiceServer) {
	s.RegisterService(&_PolarDBManagerService_serviceDesc, srv)
}

func _PolarDBManagerService_SubscribeEvent_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(PolarDBManagerServiceServer).SubscribeEvent(&polarDBManagerServiceSubscribeEventServer{stream})
}

type PolarDBManagerService_SubscribeEventServer interface {
	Send(*Response) error
	Recv() (*Request, error)
	grpc.ServerStream
}

type polarDBManagerServiceSubscribeEventServer struct {
	grpc.ServerStream
}

func (x *polarDBManagerServiceSubscribeEventServer) Send(m *Response) error {
	return x.ServerStream.SendMsg(m)
}

func (x *polarDBManagerServiceSubscribeEventServer) Recv() (*Request, error) {
	m := new(Request)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

var _PolarDBManagerService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "smartclient_service.PolarDBManagerService",
	HandlerType: (*PolarDBManagerServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "SubscribeEvent",
			Handler:       _PolarDBManagerService_SubscribeEvent_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "cluster.proto",
}

func init() { proto.RegisterFile("cluster.proto", fileDescriptor_3cfb3b8ec240c376) }

var fileDescriptor_3cfb3b8ec240c376 = []byte{
	// 522 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xac, 0x54, 0xc1, 0x8e, 0xda, 0x3a,
	0x14, 0x7d, 0x09, 0x3c, 0x48, 0xee, 0x14, 0x16, 0xae, 0x5a, 0x45, 0x33, 0x83, 0x44, 0xb3, 0x42,
	0xaa, 0x84, 0x2a, 0xba, 0xa8, 0xba, 0xe8, 0xa2, 0x74, 0x2a, 0xb5, 0x12, 0xa0, 0x2a, 0xcc, 0x1e,
	0xdd, 0x84, 0x2b, 0x88, 0x94, 0xc4, 0xa9, 0x6d, 0x86, 0x61, 0x3e, 0xa1, 0x7f, 0xd9, 0x0f, 0xe8,
	0x3f, 0x54, 0xb1, 0x1d, 0x3a, 0x48, 0x29, 0xea, 0x62, 0x76, 0x3e, 0xbe, 0xe7, 0x1c, 0x5f, 0xdb,
	0xc7, 0x86, 0x5e, 0x92, 0xed, 0xa4, 0x22, 0x31, 0x2e, 0x05, 0x57, 0x9c, 0x3d, 0x97, 0x39, 0x0a,
	0x95, 0x64, 0x29, 0x15, 0x6a, 0x25, 0x49, 0xdc, 0xa5, 0x09, 0x85, 0x7b, 0xe8, 0x46, 0xf4, 0x7d,
	0x47, 0x52, 0xb1, 0x4b, 0xf0, 0x76, 0x92, 0x44, 0x81, 0x39, 0x05, 0xce, 0xd0, 0x19, 0xf9, 0xd1,
	0x11, 0x57, 0xb5, 0x12, 0xa5, 0xdc, 0x73, 0xb1, 0x0e, 0x5c, 0x53, 0xab, 0x71, 0x55, 0x5b, 0xa3,
	0xc2, 0x18, 0x25, 0x05, 0x2d, 0x53, 0xab, 0x31, 0x0b, 0xa0, 0x9b, 0x93, 0x94, 0xb8, 0xa1, 0xa0,
	0xad, 0x4b, 0x35, 0x0c, 0x7f, 0x3a, 0xe0, 0x45, 0x24, 0x4b, 0x5e, 0x9c, 0xd2, 0x9c, 0x13, 0x1a,
	0xfb, 0x0a, 0x7d, 0xc5, 0x4b, 0x9e, 0xf1, 0xcd, 0x61, 0x45, 0x77, 0x54, 0x28, 0xbd, 0xfc, 0xc5,
	0x24, 0x1c, 0x37, 0xec, 0x66, 0x7c, 0x6b, 0xa9, 0x9f, 0x2b, 0x66, 0xd4, 0x53, 0x8f, 0x21, 0x7b,
	0x07, 0xde, 0x16, 0xad, 0x49, 0x4b, 0x9b, 0x5c, 0x37, 0x9a, 0x7c, 0xf9, 0x68, 0xe4, 0xdd, 0x2d,
	0x1e, 0x85, 0x59, 0x6c, 0x85, 0xed, 0x33, 0xc2, 0xd9, 0xd4, 0x0a, 0xb3, 0x58, 0x0f, 0xc2, 0x05,
	0xf4, 0x4e, 0x3a, 0x62, 0x1f, 0xc0, 0xab, 0x7b, 0x0a, 0x9c, 0x61, 0x6b, 0x74, 0x31, 0x79, 0x75,
	0x76, 0x1f, 0x0b, 0xbe, 0xa6, 0xe8, 0x28, 0x09, 0x7f, 0x39, 0xf0, 0xec, 0x71, 0x89, 0xbd, 0x07,
	0x2f, 0xc7, 0x7b, 0x99, 0x60, 0x46, 0xd6, 0x6f, 0xd0, 0xe8, 0x37, 0xc7, 0xfb, 0x65, 0x45, 0x8a,
	0x8e, 0x74, 0xf6, 0x1a, 0x5c, 0xb1, 0x0f, 0x5c, 0x2d, 0xba, 0x6a, 0x14, 0xdd, 0x4c, 0xf5, 0xf2,
	0xae, 0xd8, 0x6b, 0x32, 0x0f, 0x5a, 0xff, 0x42, 0xe6, 0x15, 0x39, 0xc9, 0x83, 0xf6, 0x19, 0xf2,
	0xa7, 0xb9, 0x21, 0x27, 0x39, 0xbb, 0x02, 0x3f, 0x95, 0xab, 0x1c, 0xab, 0x9c, 0x06, 0xff, 0x0f,
	0x9d, 0x91, 0x17, 0x79, 0xa9, 0x9c, 0x6b, 0x1c, 0xce, 0xa0, 0x63, 0xa8, 0xec, 0x25, 0x74, 0x04,
	0x6d, 0x52, 0x5e, 0xd8, 0x7c, 0x58, 0xc4, 0xfa, 0xe0, 0xe2, 0x83, 0x4d, 0xa4, 0x8b, 0x0f, 0x55,
	0x16, 0xa9, 0x58, 0x97, 0x3c, 0xb5, 0x77, 0xec, 0x47, 0x47, 0x1c, 0x2e, 0xc0, 0xab, 0xcf, 0xe1,
	0x49, 0xfc, 0x66, 0xd0, 0x31, 0xbb, 0x7e, 0x12, 0xb7, 0x1f, 0x0e, 0x74, 0x6d, 0xf2, 0xd8, 0x00,
	0x40, 0xa7, 0x6d, 0xa5, 0x0e, 0x65, 0xfd, 0x22, 0x7c, 0x3d, 0x73, 0x7b, 0x28, 0x89, 0x31, 0x68,
	0x6f, 0xb9, 0x54, 0xd6, 0x58, 0x8f, 0xab, 0x16, 0xa4, 0x42, 0xb5, 0x93, 0xd6, 0xd8, 0x22, 0xd3,
	0x1a, 0x4a, 0x5e, 0xd8, 0xf7, 0x67, 0x11, 0xbb, 0x06, 0x5f, 0xa5, 0x39, 0x49, 0x85, 0x79, 0xa9,
	0xcf, 0xdd, 0x8f, 0xfe, 0x4c, 0x84, 0x3e, 0x74, 0x6d, 0x98, 0x27, 0x19, 0xbc, 0xf8, 0xc6, 0x33,
	0x14, 0x37, 0xd3, 0x39, 0x16, 0xb8, 0x21, 0xb1, 0x34, 0x97, 0xc8, 0x96, 0xd0, 0x5f, 0xee, 0x62,
	0x99, 0x88, 0x34, 0x26, 0xd3, 0x76, 0xf3, 0xab, 0xb0, 0xdf, 0xcb, 0xe5, 0xe0, 0x2f, 0x55, 0xf3,
	0x05, 0x84, 0xff, 0x8d, 0x9c, 0x37, 0x4e, 0xdc, 0xd1, 0x5f, 0xd5, 0xdb, 0xdf, 0x01, 0x00, 0x00,
	0xff, 0xff, 0x9d, 0x40, 0x6f, 0x69, 0xbb, 0x04, 0x00, 0x00,
}
