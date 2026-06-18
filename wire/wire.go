package wire

import (
	"github.com/google/wire"
	"github.com/xiaoxiaodek/kafkaflow-go/builder"
	"github.com/xiaoxiaodek/kafkaflow-go/log"
)

// CoreSet provides the core KafkaFlow dependencies.
var CoreSet = wire.NewSet(
	log.DefaultLogger,
	builder.NewConfig,
	builder.NewBus,
)
