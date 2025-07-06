package proto_utilities

import (
	"encoding/json"
	"github.com/scusemua/workload-driver-react/m/v2/internal/server/api/proto"
	"github.com/scusemua/workload-driver-react/m/v2/pkg/jupyter"
)

// ToProtoJupyterMessageHeader creates a new *proto.JupyterMessageHeader struct with the data from the corresponding
// fields of the given *jupyter.KernelMessageHeader struct.
func ToProtoJupyterMessageHeader(header *jupyter.KernelMessageHeader) *proto.JupyterMessageHeader {
	if header == nil {
		return nil
	}

	protoHeader := &proto.JupyterMessageHeader{}

	protoHeader.MessageId = header.MessageId
	protoHeader.Username = header.Username
	protoHeader.Session = header.Session
	protoHeader.Date = header.Date
	protoHeader.MessageType = header.MessageType.String()
	protoHeader.Version = header.Version
	protoHeader.SubshellId = header.SubshellId

	return protoHeader
}

// FromProtoJupyterMessageHeader creates a new *jupyter.KernelMessageHeader struct with the data from the corresponding
// fields of the given proto.JupyterMessageHeader struct.
func FromProtoJupyterMessageHeader(protoHeader *proto.JupyterMessageHeader) *jupyter.KernelMessageHeader {
	if protoHeader == nil {
		return nil
	}

	header := &jupyter.KernelMessageHeader{}

	header.MessageId = protoHeader.MessageId
	header.Username = protoHeader.Username
	header.Session = protoHeader.Session
	header.Date = protoHeader.Date
	header.MessageType = jupyter.MessageType(protoHeader.MessageType)
	header.Version = protoHeader.Version
	header.SubshellId = protoHeader.SubshellId

	return header
}

// ProtoToJupyterMessage creates and returns a new jupyter.Message struct created using the data from the
// corresponding fields of the given proto.JupyterMessage struct.
func ProtoToJupyterMessage(protoMsg *proto.JupyterMessage) (*jupyter.Message, error) {
	var (
		header, parentHeader        *jupyter.KernelMessageHeader
		metadataFrame, contentFrame map[string]interface{}
		err                         error
	)

	if protoMsg.Header != nil {
		header = FromProtoJupyterMessageHeader(protoMsg.Header)
	}

	if protoMsg.ParentHeader != nil {
		parentHeader = FromProtoJupyterMessageHeader(protoMsg.ParentHeader)
	}

	if protoMsg.Metadata != nil {
		err = json.Unmarshal(protoMsg.Metadata, &metadataFrame)
		if err != nil {
			return nil, err
		}
	}

	if protoMsg.Content != nil {
		err = json.Unmarshal(protoMsg.Content, &contentFrame)
		if err != nil {
			return nil, err
		}
	}

	msg := &jupyter.Message{
		Header:       header,
		ParentHeader: parentHeader,
		Metadata:     metadataFrame,
		Content:      contentFrame,
		Buffers:      protoMsg.Buffers,
	}

	return msg, nil
}

// JupyterMessageToProto creates and returns a new *proto.JupyterMessage struct created using the data from the
// corresponding fields of the given jupyter.Message struct.
func JupyterMessageToProto(msg jupyter.Message) (*proto.JupyterMessage, error) {
	var (
		protoHeader, protoParentHeader *proto.JupyterMessageHeader
		metadataFrame, contentFrame    []byte
		err                            error
	)

	if msg.Header != nil {
		protoHeader = ToProtoJupyterMessageHeader(msg.Header)
	}

	if msg.ParentHeader != nil {
		protoParentHeader = ToProtoJupyterMessageHeader(msg.ParentHeader)
	}

	if msg.Metadata != nil {
		metadataFrame, err = json.Marshal(msg.Metadata)
		if err != nil {
			return nil, err
		}
	}

	if msg.Content != nil {
		contentFrame, err = json.Marshal(msg.Content)
		if err != nil {
			return nil, err
		}
	}

	protoMessage := &proto.JupyterMessage{
		Header:       protoHeader,
		ParentHeader: protoParentHeader,
		Metadata:     metadataFrame,
		Content:      contentFrame,
		Buffers:      msg.Buffers,
	}

	return protoMessage, nil
}
