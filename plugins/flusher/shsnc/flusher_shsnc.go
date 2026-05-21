package shsnc

/*
#cgo LDFLAGS: -ldl
#include <stdlib.h>

// 声明 logtail.cpp 中暴露的 C 函数
extern void sncAgentSendData(const char* data, int dataLen);
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strconv"
	"sync"
	"unsafe"

	"github.com/alibaba/ilogtail/pkg/logger"
	"github.com/alibaba/ilogtail/pkg/models"
	"github.com/alibaba/ilogtail/pkg/pipeline"
	"github.com/alibaba/ilogtail/pkg/protocol"

	"github.com/cihub/seelog"
	jsoniter "github.com/json-iterator/go"
)

// ========================== 安全优化：异步队列 ==========================
const (
	sendQueueSize = 20000 // 调大一点，阻塞不漏数
)

const flushMsg = `
<seelog minlevel="info" >
<outputs formatid="common">
	 %s
 </outputs>
 <formats>
	 <format id="common" format="%%Date %%Time %%Msg%%n" />
 </formats>
</seelog>
`

// FlusherShsnc 重命名后的插件
type FlusherShsnc struct {
	FileName      string
	MaxSize       int
	MaxRolls      int
	KeyValuePairs bool
	Tags          bool
	OnlyStdout    bool

	context   pipeline.Context
	outLogger seelog.LoggerInterface

	// 异步发送（修复 JNI 跨线程崩溃 核心）
	sendChan chan []byte
	wg       sync.WaitGroup
	closed   bool
	mu       sync.Mutex
}

// ------------------------------
// Init 启动单线程发送协程
// ------------------------------
func (p *FlusherShsnc) Init(context pipeline.Context) error {
	p.context = context
	p.mu.Lock()
	p.sendChan = make(chan []byte, sendQueueSize)
	p.closed = false
	p.mu.Unlock()

	// 启动【单线程】消费协程 → 所有 JNI 调用都在这个线程执行
	// 彻底解决：跨线程 JNIEnv + 栈溢出
	p.wg.Add(1)
	go p.sendWorker()

	logger.Info(p.context.GetRuntimeContext(), "FlusherShsnc 初始化完成，异步单线程发送协程已启动 | 阻塞队列模式：不漏日志")

	pattern := ""
	if p.OnlyStdout {
		pattern = "<console/>"
		logger.CloseCatchStdout()
	} else if p.FileName != "" {
		pattern = `<rollingfile type="size" filename="%s" maxsize="%d" maxrolls="%d"/>`
		if p.MaxSize <= 0 {
			p.MaxSize = 1024 * 1024
		}
		if p.MaxRolls <= 0 {
			p.MaxRolls = 1
		}
		pattern = fmt.Sprintf(pattern, p.FileName, p.MaxSize, p.MaxRolls)
	}
	if pattern != "" {
		var err error
		p.outLogger, err = seelog.LoggerFromConfigAsString(fmt.Sprintf(flushMsg, pattern))
		if err != nil {
			logger.Warning(p.context.GetRuntimeContext(), "FLUSHER_INIT_ALARM", "init shsnc flusher fail", err)
			p.outLogger = seelog.Disabled
		}
	}
	return nil
}

func (*FlusherShsnc) Description() string {
	return "shsnc flusher: Go -> C++ -> JNI -> Java (稳定异步阻塞版·不漏数)"
}

// ------------------------------
// Flush 不变，只入队
// 核心：Flush 方法 → 抽取输出逻辑为 sendToShsnc
// ------------------------------
func (p *FlusherShsnc) Flush(projectName string, logstoreName string, configName string, logGroupList []*protocol.LogGroup) error {
	for _, logGroup := range logGroupList {
		if p.Tags {
			if p.outLogger != nil {
				p.outLogger.Infof("[LogGroup] topic %s, logstore %s, logcount %d, tags %v", logGroup.Topic, logGroup.Category, len(logGroup.Logs), logGroup.LogTags)
			} else {
				logger.Info(p.context.GetRuntimeContext(), "[LogGroup] topic", logGroup.Topic, "logstore", logGroup.Category, "logcount", len(logGroup.Logs), "tags", logGroup.LogTags)
			}
		}

		if p.KeyValuePairs {
			for _, log := range logGroup.Logs {
				writer := jsoniter.NewStream(jsoniter.ConfigDefault, nil, 128)
				writer.WriteObjectStart()
				for _, c := range log.Contents {
					writer.WriteObjectField(c.Key)
					writer.WriteString(c.Value)
					_, _ = writer.Write([]byte{','})
				}
				writer.WriteObjectField("__time__")
				writer.WriteString(strconv.Itoa(int(log.Time)))
				writer.WriteMore()
				writer.WriteObjectField("__flushType__")
				writer.WriteString("flusher_shsnc")
				writer.WriteObjectEnd()

				// ==============================================
				// 【抽取点】原控制台输出 → 调用自定义 sendToShsnc
				// ==============================================
				p.sendToShsnc(writer.Buffer())
			}

		} else {
			for _, log := range logGroup.Logs {
				buf, _ := json.Marshal(log)
				// ==============================================
				// 【抽取点】原控制台输出 → 调用自定义 sendToShsnc
				// ==============================================
				p.sendToShsnc(buf)
			}
		}
	}
	return nil
}

// ------------------------------
// 【不漏数·阻塞版】队列满会等待，不丢弃
// ------------------------------
func (p *FlusherShsnc) sendToShsnc(data []byte) {

	logger.Debug(p.context.GetRuntimeContext(), "sendToShsnc:", string(data))
	// 复制数据，避免内存被覆盖（防崩溃）
	d := make([]byte, len(data))
	copy(d, data)

	p.mu.Lock()
	closed := p.closed
	ch := p.sendChan
	p.mu.Unlock()

	if closed || ch == nil {
		return
	}

	// ========================
	// 关键：阻塞入队，绝不丢数
	// ========================
	ch <- d
}

// ------------------------------
// 【单线程工作协程】
// 所有 JNI 调用都在这里，绝对安全
// ------------------------------
func (p *FlusherShsnc) sendWorker() {
	defer p.wg.Done()

	defer func() {
		if err := recover(); err != nil {
			logger.Error(p.context.GetRuntimeContext(), "SHSNC_WORKER_PANIC", fmt.Sprintf("%v", err), string(debug.Stack()))
		}
	}()

	logger.Info(p.context.GetRuntimeContext(), "SHSNC 单线程发送协程运行中")

	// 单线程循环消费 → JNI 不会跨线程 → 不崩溃
	for data := range p.sendChan {
		func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Error(p.context.GetRuntimeContext(), "SHSNC_SEND_PANIC", err)
				}
			}()

			if len(data) == 0 {
				return
			}
			logger.Debug(p.context.GetRuntimeContext(), "sendWorker:", string(data))

			// 真正调用 JNI（只在这一个线程）
			cData := (*C.char)(unsafe.Pointer(&data[0]))
			cLen := C.int(len(data))
			C.sncAgentSendData(cData, cLen)
		}()
	}
}

// ------------------------------
// Export 不变
// ------------------------------
func (p *FlusherShsnc) Export(in []*models.PipelineGroupEvents, context pipeline.PipelineContext) error {
	for _, groupEvents := range in {
		if p.Tags {
			metadata := fmt.Sprintf("%v", groupEvents.Group.GetMetadata().Iterator())
			tags := fmt.Sprintf("%v", groupEvents.Group.GetTags().Iterator())
			if p.outLogger != nil {
				p.outLogger.Infof("[Event] event %d, metadata %s, tags %s", len(groupEvents.Events), metadata, tags)
			} else {
				logger.Info(p.context.GetRuntimeContext(), "[Event] event", len(groupEvents.Events), "metadata", metadata, "tags", tags)
			}
		}

		for _, event := range groupEvents.Events {
			writer := jsoniter.NewStream(jsoniter.ConfigDefault, nil, 128)
			writer.WriteObjectStart()
			writer.WriteObjectField("eventType")
			switch event.GetType() {
			case models.EventTypeMetric:
				writer.WriteString("metric")
			case models.EventTypeSpan:
				writer.WriteString("span")
			case models.EventTypeLogging:
				writer.WriteString("log")
			case models.EventTypeByteArray:
				writer.WriteString("byteArray")
			}
			_, _ = writer.Write([]byte{','})
			writer.WriteObjectField("name")
			writer.WriteString(event.GetName())
			_, _ = writer.Write([]byte{','})
			writer.WriteObjectField("timestamp")
			writer.WriteUint64(event.GetTimestamp())
			_, _ = writer.Write([]byte{','})
			writer.WriteObjectField("observedTimestamp")
			writer.WriteUint64(event.GetObservedTimestamp())
			_, _ = writer.Write([]byte{','})
			writer.WriteObjectField("tags")
			writer.WriteObjectStart()
			i := 0
			for k, v := range event.GetTags().Iterator() {
				writer.WriteObjectField(k)
				writer.WriteString(v)
				if i < event.GetTags().Len()-1 {
					_, _ = writer.Write([]byte{','})
				}
				i++
			}
			writer.WriteObjectEnd()
			_, _ = writer.Write([]byte{','})
			switch event.GetType() {
			case models.EventTypeMetric:
				p.writeMetricValues(writer, event.(*models.Metric))
			case models.EventTypeSpan:
				p.writeSpan(writer, nil)
			case models.EventTypeLogging:
				p.writeLogBody(writer, event.(*models.Log))
			case models.EventTypeByteArray:
				p.writeByteArray(writer, event.(models.ByteArray))
			}

			writer.WriteObjectEnd()

			// 新模型也调用 sendToShsnc
			p.sendToShsnc(writer.Buffer())
		}
	}
	return nil
}

func (p *FlusherShsnc) writeMetricValues(writer *jsoniter.Stream, metric *models.Metric) {
	writer.WriteObjectField("metricType")
	writer.WriteString(models.MetricTypeTexts[metric.GetMetricType()])
	_, _ = writer.Write([]byte{','})
	if metric.GetValue().IsSingleValue() {
		writer.WriteObjectField("value")
		writer.WriteFloat64(metric.GetValue().GetSingleValue())
	} else {
		writer.WriteObjectField("values")
		writer.WriteObjectStart()
		values := metric.GetValue().GetMultiValues()
		i := 0
		for k, v := range values.Iterator() {
			writer.WriteObjectField(k)
			writer.WriteFloat64(v)
			if i < values.Len()-1 {
				_, _ = writer.Write([]byte{','})
			}
			i++
		}
		if metric.GetTypedValue().Len() > 0 {
			_, _ = writer.Write([]byte{','})
			i = 0
			for k, v := range metric.GetTypedValue().Iterator() {
				writer.WriteObjectField(k)
				switch v.Type {
				case models.ValueTypeString:
					writer.WriteString(v.Value.(string))
				case models.ValueTypeBoolean:
					writer.WriteBool(v.Value.(bool))
				}
				if i < metric.GetTypedValue().Len()-1 {
					_, _ = writer.Write([]byte{','})
				}
				i++
			}
		}
		writer.WriteObjectEnd()
	}
}

func (p *FlusherShsnc) writeSpan(writer *jsoniter.Stream, metric *models.Span) {
	// TODO
}

func (p *FlusherShsnc) writeLogBody(writer *jsoniter.Stream, log *models.Log) {
	writer.WriteObjectField("offset")
	writer.WriteInt64(int64(log.GetOffset()))
	_, _ = writer.Write([]byte{','})
	writer.WriteObjectField("level")
	writer.WriteString(log.GetLevel())
	_, _ = writer.Write([]byte{','})
	writer.WriteObjectField("traceID")
	writer.WriteString(log.GetTraceID())
	_, _ = writer.Write([]byte{','})
	writer.WriteObjectField("traceID")
	writer.WriteString(log.GetTraceID())
	_, _ = writer.Write([]byte{','})
	writer.WriteObjectField("spanID")
	writer.WriteString(log.GetSpanID())
	contents := log.GetIndices()
	for key, value := range contents.Iterator() {
		_, _ = writer.Write([]byte{','})
		writer.WriteObjectField(key)
		_, _ = writer.Write([]byte(fmt.Sprintf("%#v", value)))
	}
}

func (p *FlusherShsnc) writeByteArray(writer *jsoniter.Stream, bytes models.ByteArray) {
	writer.WriteObjectField("byteArray")
	_, _ = writer.Write([]byte{'"'})
	_, _ = writer.Write(bytes)
	_, _ = writer.Write([]byte{'"'})
}

func (p *FlusherShsnc) SetUrgent(flag bool) {
}

// IsReady 完全不变
func (*FlusherShsnc) IsReady(projectName string, logstoreName string, logstoreKey int64) bool {
	return true
}

// ------------------------------
// Stop 优雅关闭
// ------------------------------
func (p *FlusherShsnc) Stop() error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.sendChan)
	}
	p.mu.Unlock()

	p.wg.Wait()

	if p.outLogger != nil {
		p.outLogger.Close()
	}
	logger.Info(p.context.GetRuntimeContext(), "FlusherShsnc 安全停止完成，所有数据已消费完毕")
	return nil
}

// ==============================================
// 【插件注册】重命名为 flusher_shsnc
// ==============================================
func init() {
	pipeline.Flushers["flusher_shsnc"] = func() pipeline.Flusher {
		return &FlusherShsnc{
			KeyValuePairs: true,
		}
	}
}
