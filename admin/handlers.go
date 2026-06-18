package admin

import (
	"context"
	"fmt"

	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer"
	"github.com/xiaoxiaodek/kafkaflow-go/log"
)

// ConsumerRegistry provides access to consumer managers by name and group.
type ConsumerRegistry interface {
	GetByName(name string) (*consumer.ConsumerManager, bool)
	GetByGroup(groupID string) []*consumer.ConsumerManager
	AllConsumers() []*consumer.ConsumerManager
}

// AdminHandler dispatches admin messages to the appropriate handler.
type AdminHandler struct {
	registry ConsumerRegistry
	logger   log.Logger
}

// NewAdminHandler creates a new AdminHandler.
func NewAdminHandler(registry ConsumerRegistry, logger log.Logger) *AdminHandler {
	if logger == nil {
		logger = log.DefaultLogger()
	}
	return &AdminHandler{registry: registry, logger: logger}
}

// HandleMessage deserializes and dispatches an admin message.
func (h *AdminHandler) HandleMessage(ctx context.Context, mc *kafkaflow.MessageContext) error {
	msgType := ""
	for _, hdr := range mc.Message.Headers {
		if hdr.Key == "message-type" {
			msgType = string(hdr.Value)
			break
		}
	}

	msg, err := decodeAdminMessage(msgType, messageBytes(mc.Message.Value))
	if err != nil {
		h.logger.Warn(ctx, "unknown admin message type", "type", msgType)
		return nil
	}

	switch m := msg.(type) {
	case *PauseConsumerByName:
		return h.handlePauseConsumerByName(ctx, *m)
	case *ResumeConsumerByName:
		return h.handleResumeConsumerByName(ctx, *m)
	case *StartConsumerByName:
		return h.handleStartConsumerByName(ctx, *m)
	case *StopConsumerByName:
		return h.handleStopConsumerByName(ctx, *m)
	case *RestartConsumerByName:
		return h.handleRestartConsumerByName(ctx, *m)
	case *ResetConsumerOffset:
		return h.handleResetConsumerOffset(ctx, *m)
	case *RewindConsumerOffsetToDateTime:
		return h.handleRewindConsumerOffset(ctx, *m)
	case *ChangeConsumerWorkersCount:
		return h.handleChangeWorkersCount(ctx, *m)
	case *PauseConsumersByGroup:
		return h.handlePauseConsumersByGroup(ctx, *m)
	case *ResumeConsumersByGroup:
		return h.handleResumeConsumersByGroup(ctx, *m)
	default:
		h.logger.Warn(ctx, "unknown admin message type", "type", msgType)
		return nil
	}
}

func messageBytes(value interface{}) []byte {
	if data, ok := value.([]byte); ok {
		return data
	}
	return nil
}

func (h *AdminHandler) handlePauseConsumerByName(ctx context.Context, msg PauseConsumerByName) error {
	cm, ok := h.registry.GetByName(msg.ConsumerName)
	if !ok {
		h.logger.Warn(ctx, "consumer not found", "name", msg.ConsumerName)
		return nil
	}
	return cm.Pause(msg.Topics)
}

func (h *AdminHandler) handleResumeConsumerByName(ctx context.Context, msg ResumeConsumerByName) error {
	cm, ok := h.registry.GetByName(msg.ConsumerName)
	if !ok {
		h.logger.Warn(ctx, "consumer not found", "name", msg.ConsumerName)
		return nil
	}
	return cm.Resume(msg.Topics)
}

func (h *AdminHandler) handleStartConsumerByName(ctx context.Context, msg StartConsumerByName) error {
	cm, ok := h.registry.GetByName(msg.ConsumerName)
	if !ok {
		h.logger.Warn(ctx, "consumer not found", "name", msg.ConsumerName)
		return nil
	}
	return cm.Start(ctx)
}

func (h *AdminHandler) handleStopConsumerByName(ctx context.Context, msg StopConsumerByName) error {
	cm, ok := h.registry.GetByName(msg.ConsumerName)
	if !ok {
		h.logger.Warn(ctx, "consumer not found", "name", msg.ConsumerName)
		return nil
	}
	cm.Stop()
	return nil
}

func (h *AdminHandler) handleRestartConsumerByName(ctx context.Context, msg RestartConsumerByName) error {
	cm, ok := h.registry.GetByName(msg.ConsumerName)
	if !ok {
		h.logger.Warn(ctx, "consumer not found", "name", msg.ConsumerName)
		return nil
	}
	return cm.Restart()
}

func (h *AdminHandler) handleResetConsumerOffset(ctx context.Context, msg ResetConsumerOffset) error {
	cm, ok := h.registry.GetByName(msg.ConsumerName)
	if !ok {
		h.logger.Warn(ctx, "consumer not found", "name", msg.ConsumerName)
		return nil
	}
	return cm.ResetOffsets(msg.Topics)
}

func (h *AdminHandler) handleRewindConsumerOffset(ctx context.Context, msg RewindConsumerOffsetToDateTime) error {
	cm, ok := h.registry.GetByName(msg.ConsumerName)
	if !ok {
		h.logger.Warn(ctx, "consumer not found", "name", msg.ConsumerName)
		return nil
	}
	return cm.RewindOffsets(msg.Topics, msg.Timestamp)
}

func (h *AdminHandler) handleChangeWorkersCount(ctx context.Context, msg ChangeConsumerWorkersCount) error {
	cm, ok := h.registry.GetByName(msg.ConsumerName)
	if !ok {
		h.logger.Warn(ctx, "consumer not found", "name", msg.ConsumerName)
		return nil
	}
	return cm.ChangeWorkersCount(msg.WorkersCount)
}

func (h *AdminHandler) handlePauseConsumersByGroup(ctx context.Context, msg PauseConsumersByGroup) error {
	for _, cm := range h.registry.GetByGroup(msg.GroupID) {
		if err := cm.Pause(msg.Topics); err != nil {
			return fmt.Errorf("failed to pause consumer %s: %w", cm.Name(), err)
		}
	}
	return nil
}

func (h *AdminHandler) handleResumeConsumersByGroup(ctx context.Context, msg ResumeConsumersByGroup) error {
	for _, cm := range h.registry.GetByGroup(msg.GroupID) {
		if err := cm.Resume(msg.Topics); err != nil {
			return fmt.Errorf("failed to resume consumer %s: %w", cm.Name(), err)
		}
	}
	return nil
}
