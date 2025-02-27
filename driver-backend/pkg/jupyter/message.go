package jupyter

import (
	"encoding/json"
	"time"
)

const (
	ShellChannel   KernelSocketChannel = "shell"
	ControlChannel KernelSocketChannel = "control"
	IOPubChannel   KernelSocketChannel = "iopub"
	StdinChannel   KernelSocketChannel = "stdin"
)

type KernelSocketChannel string

func (s KernelSocketChannel) String() string {
	return string(s)
}

type KernelMessage interface {
	GetHeader() *KernelMessageHeader
	GetChannel() KernelSocketChannel
	GetContent() interface{}
	GetBuffers() [][]byte
	GetMetadata() map[string]interface{}
	GetParentHeader() *KernelMessageHeader
	DecodeContent() (map[string]interface{}, error)

	// AddMetadata adds metadata with the given key and value to the underlying message.
	//
	// If there already exists an entry with the given key, then that entry is silently overwritten.
	AddMetadata(key string, value interface{})

	String() string
}

// ResourceSpec can be passed within a jupyterSessionReq when creating a new Session or Kernel.
type ResourceSpec struct {
	Cpu  int     `json:"cpu"`    // In millicpus (1/1000th CPU core)
	Mem  float64 `json:"memory"` // In MB
	Gpu  int     `json:"gpu"`
	Vram float64 `json:"vram"` // In GB
}

func (s *ResourceSpec) String() string {
	m, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}

	return string(m)
}

// MessageBuilder structs are used to construct Message structs.
type MessageBuilder struct {
	channel      KernelSocketChannel
	header       *KernelMessageHeader
	parentHeader *KernelMessageHeader
	metadata     map[string]interface{}
	content      interface{}
	buffers      [][]byte
}

func NewMessageBuilder() *MessageBuilder {
	return &MessageBuilder{
		metadata:     make(map[string]interface{}),
		buffers:      make([][]byte, 0),
		header:       &KernelMessageHeader{},
		parentHeader: &KernelMessageHeader{},
	}
}

func (b *MessageBuilder) WithChannel(channel KernelSocketChannel) *MessageBuilder {
	b.channel = channel
	return b
}

func (b *MessageBuilder) WithHeader(header *KernelMessageHeader) *MessageBuilder {
	b.header = header
	return b
}

func (b *MessageBuilder) WithParentHeader(parentHeader *KernelMessageHeader) *MessageBuilder {
	b.parentHeader = parentHeader
	return b
}

// WithMetadata adds a piece of metadata to the metadata dictionary.
//
// If a metadata value already exists for the given key, then that value is overwritten.
func (b *MessageBuilder) WithMetadata(key string, value interface{}) *MessageBuilder {
	if b.metadata == nil {
		b.metadata = make(map[string]interface{})
	}

	b.metadata[key] = value
	return b
}

// WithMetadataDictionary replaces the metadata dictionary registered with the MessageBuilder
// with the given metadata dictionary.
func (b *MessageBuilder) WithMetadataDictionary(metadata map[string]interface{}) *MessageBuilder {
	b.metadata = metadata
	return b
}

func (b *MessageBuilder) WithContent(content interface{}) *MessageBuilder {
	b.content = content
	return b
}

// WithBuffers replaces the buffers registered with the MessageBuilder with the given buffers.
func (b *MessageBuilder) WithBuffers(buffers [][]byte) *MessageBuilder {
	b.buffers = buffers
	return b
}

// WithBuffer appends a buffer to the buffers registered with the MessageBuilder.
func (b *MessageBuilder) WithBuffer(buffer []byte) *MessageBuilder {
	if b.buffers == nil {
		b.buffers = make([][]byte, 0)
	}

	b.buffers = append(b.buffers, buffer)
	return b
}

func (b *MessageBuilder) Build() *Message {
	buffers := b.buffers
	if buffers == nil {
		buffers = make([][]byte, 0)
	}

	header := b.header
	if header == nil {
		header = &KernelMessageHeader{}
	}

	parentHeader := b.parentHeader
	if parentHeader == nil {
		parentHeader = &KernelMessageHeader{}
	}

	message := &Message{
		Channel:      b.channel,
		Header:       header,
		ParentHeader: parentHeader,
		Metadata:     b.metadata,
		Content:      b.content,
		Buffers:      buffers,
	}

	return message
}

type Message struct {
	Channel      KernelSocketChannel    `json:"channel"`
	Header       *KernelMessageHeader   `json:"header"`
	ParentHeader *KernelMessageHeader   `json:"parent_header"`
	Metadata     map[string]interface{} `json:"metadata"`
	Content      interface{}            `json:"content"`
	Buffers      [][]byte               `json:"buffers"`
}

func (m *Message) GetHeader() *KernelMessageHeader {
	return m.Header
}

func (m *Message) DecodeContent() (map[string]interface{}, error) {
	var content map[string]interface{}
	err := json.Unmarshal(m.Content.([]byte), &content)
	return content, err
}

func (m *Message) GetChannel() KernelSocketChannel {
	return m.Channel
}

func (m *Message) GetContent() interface{} {
	return m.Content
}

func (m *Message) GetBuffers() [][]byte {
	return m.Buffers
}

// AddMetadata adds metadata with the given key and value to the underlying message.
//
// If there already exists an entry with the given key, then that entry is silently overwritten.
func (m *Message) AddMetadata(key string, value interface{}) {
	if m.Metadata == nil {
		m.Metadata = make(map[string]interface{})
	}

	m.Metadata[key] = value
}

func (m *Message) GetMetadata() map[string]interface{} {
	return m.Metadata
}

func (m *Message) GetParentHeader() *KernelMessageHeader {
	return m.ParentHeader
}

func (m *Message) String() string {
	out, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}

	return string(out)
}

// KernelMessageHeaderBuilder is used to construct KernelMessageHeader structs.
//
// If the date or the version are not specified, then default values will be used automatically.
//
// For date, the default value is time.Now().Format(JavascriptISOString).
//
// For version, the default value is VERSION.
type KernelMessageHeaderBuilder struct {
	date        string
	messageId   string
	messageType MessageType
	session     string
	username    string
	version     string
}

func NewKernelMessageHeaderBuilder() *KernelMessageHeaderBuilder {
	return &KernelMessageHeaderBuilder{}
}

func (b *KernelMessageHeaderBuilder) WithDate(date time.Time) *KernelMessageHeaderBuilder {
	b.date = date.UTC().Format(JavascriptISOString)
	return b
}

func (b *KernelMessageHeaderBuilder) WithDateString(date string) *KernelMessageHeaderBuilder {
	b.date = date
	return b
}

func (b *KernelMessageHeaderBuilder) WithMessageId(messageId string) *KernelMessageHeaderBuilder {
	b.messageId = messageId
	return b
}

func (b *KernelMessageHeaderBuilder) WithMessageType(messageType MessageType) *KernelMessageHeaderBuilder {
	b.messageType = messageType
	return b
}

func (b *KernelMessageHeaderBuilder) WithSession(session string) *KernelMessageHeaderBuilder {
	b.session = session
	return b
}

func (b *KernelMessageHeaderBuilder) WithUsername(username string) *KernelMessageHeaderBuilder {
	b.username = username
	return b
}

func (b *KernelMessageHeaderBuilder) WithVersion(version string) *KernelMessageHeaderBuilder {
	b.version = version
	return b
}

func (b *KernelMessageHeaderBuilder) Build() *KernelMessageHeader {
	version := b.version
	if version == "" {
		version = VERSION
	}

	date := b.date
	if date == "" {
		date = time.Now().Format(JavascriptISOString)
	}

	return &KernelMessageHeader{
		Date:        date,
		MessageId:   b.messageId,
		MessageType: b.messageType,
		Session:     b.session,
		Username:    b.username,
		Version:     version,
	}
}

type KernelMessageHeader struct {
	Date        string      `json:"date"`
	MessageId   string      `json:"msg_id"`
	MessageType MessageType `json:"msg_type"`
	Session     string      `json:"session"`
	Username    string      `json:"username"`
	Version     string      `json:"version"`
	SubshellId  *string     `json:"subshell_id,omitempty"`
}

func (h *KernelMessageHeader) String() string {
	out, err := json.Marshal(h)
	if err != nil {
		panic(err)
	}

	return string(out)
}
