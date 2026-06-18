package admin

import (
	"fmt"

	"github.com/xiaoxiaodek/kafkaflow-go/admin/adminpb"
	"google.golang.org/protobuf/proto"
)

// EncodeAdminMessage encodes an admin message using protobuf.
func EncodeAdminMessage(msg AdminMessage) ([]byte, error) {
	return encodeAdminMessage(msg)
}

func encodeAdminMessage(msg AdminMessage) ([]byte, error) {
	cmd := toProtoCommand(msg)
	if cmd == nil {
		return nil, fmt.Errorf("unknown admin message type %T", msg)
	}
	return proto.Marshal(cmd)
}

func decodeAdminMessage(messageType string, data []byte) (AdminMessage, error) {
	var cmd adminpb.AdminCommand
	if err := proto.Unmarshal(data, &cmd); err != nil {
		return nil, fmt.Errorf("admin: failed to unmarshal protobuf command: %w", err)
	}
	return fromProtoCommand(&cmd)
}

func toProtoCommand(msg AdminMessage) *adminpb.AdminCommand {
	switch m := msg.(type) {
	case *PauseConsumerByName:
		return &adminpb.AdminCommand{
			Command: &adminpb.AdminCommand_PauseConsumerByName{
				PauseConsumerByName: &adminpb.PauseConsumerByName{
					ConsumerName: m.ConsumerName,
					Topics:       m.Topics,
				},
			},
		}
	case *ResumeConsumerByName:
		return &adminpb.AdminCommand{
			Command: &adminpb.AdminCommand_ResumeConsumerByName{
				ResumeConsumerByName: &adminpb.ResumeConsumerByName{
					ConsumerName: m.ConsumerName,
					Topics:       m.Topics,
				},
			},
		}
	case *StartConsumerByName:
		return &adminpb.AdminCommand{
			Command: &adminpb.AdminCommand_StartConsumerByName{
				StartConsumerByName: &adminpb.StartConsumerByName{
					ConsumerName: m.ConsumerName,
				},
			},
		}
	case *StopConsumerByName:
		return &adminpb.AdminCommand{
			Command: &adminpb.AdminCommand_StopConsumerByName{
				StopConsumerByName: &adminpb.StopConsumerByName{
					ConsumerName: m.ConsumerName,
				},
			},
		}
	case *RestartConsumerByName:
		return &adminpb.AdminCommand{
			Command: &adminpb.AdminCommand_RestartConsumerByName{
				RestartConsumerByName: &adminpb.RestartConsumerByName{
					ConsumerName: m.ConsumerName,
				},
			},
		}
	case *ResetConsumerOffset:
		return &adminpb.AdminCommand{
			Command: &adminpb.AdminCommand_ResetConsumerOffset{
				ResetConsumerOffset: &adminpb.ResetConsumerOffset{
					ConsumerName: m.ConsumerName,
					Topics:       m.Topics,
				},
			},
		}
	case *RewindConsumerOffsetToDateTime:
		return &adminpb.AdminCommand{
			Command: &adminpb.AdminCommand_RewindConsumerOffsetToDateTime{
				RewindConsumerOffsetToDateTime: &adminpb.RewindConsumerOffsetToDateTime{
					ConsumerName: m.ConsumerName,
					Topics:       m.Topics,
					Timestamp:    m.Timestamp,
				},
			},
		}
	case *ChangeConsumerWorkersCount:
		return &adminpb.AdminCommand{
			Command: &adminpb.AdminCommand_ChangeConsumerWorkersCount{
				ChangeConsumerWorkersCount: &adminpb.ChangeConsumerWorkersCount{
					ConsumerName: m.ConsumerName,
					WorkersCount: int32(m.WorkersCount),
				},
			},
		}
	case *PauseConsumersByGroup:
		return &adminpb.AdminCommand{
			Command: &adminpb.AdminCommand_PauseConsumersByGroup{
				PauseConsumersByGroup: &adminpb.PauseConsumersByGroup{
					GroupId: m.GroupID,
					Topics:  m.Topics,
				},
			},
		}
	case *ResumeConsumersByGroup:
		return &adminpb.AdminCommand{
			Command: &adminpb.AdminCommand_ResumeConsumersByGroup{
				ResumeConsumersByGroup: &adminpb.ResumeConsumersByGroup{
					GroupId: m.GroupID,
					Topics:  m.Topics,
				},
			},
		}
	default:
		return nil
	}
}

func fromProtoCommand(cmd *adminpb.AdminCommand) (AdminMessage, error) {
	switch c := cmd.Command.(type) {
	case *adminpb.AdminCommand_PauseConsumerByName:
		return &PauseConsumerByName{
			ConsumerName: c.PauseConsumerByName.ConsumerName,
			Topics:       c.PauseConsumerByName.Topics,
		}, nil
	case *adminpb.AdminCommand_ResumeConsumerByName:
		return &ResumeConsumerByName{
			ConsumerName: c.ResumeConsumerByName.ConsumerName,
			Topics:       c.ResumeConsumerByName.Topics,
		}, nil
	case *adminpb.AdminCommand_StartConsumerByName:
		return &StartConsumerByName{
			ConsumerName: c.StartConsumerByName.ConsumerName,
		}, nil
	case *adminpb.AdminCommand_StopConsumerByName:
		return &StopConsumerByName{
			ConsumerName: c.StopConsumerByName.ConsumerName,
		}, nil
	case *adminpb.AdminCommand_RestartConsumerByName:
		return &RestartConsumerByName{
			ConsumerName: c.RestartConsumerByName.ConsumerName,
		}, nil
	case *adminpb.AdminCommand_ResetConsumerOffset:
		return &ResetConsumerOffset{
			ConsumerName: c.ResetConsumerOffset.ConsumerName,
			Topics:       c.ResetConsumerOffset.Topics,
		}, nil
	case *adminpb.AdminCommand_RewindConsumerOffsetToDateTime:
		return &RewindConsumerOffsetToDateTime{
			ConsumerName: c.RewindConsumerOffsetToDateTime.ConsumerName,
			Topics:       c.RewindConsumerOffsetToDateTime.Topics,
			Timestamp:    c.RewindConsumerOffsetToDateTime.Timestamp,
		}, nil
	case *adminpb.AdminCommand_ChangeConsumerWorkersCount:
		return &ChangeConsumerWorkersCount{
			ConsumerName: c.ChangeConsumerWorkersCount.ConsumerName,
			WorkersCount: int(c.ChangeConsumerWorkersCount.WorkersCount),
		}, nil
	case *adminpb.AdminCommand_PauseConsumersByGroup:
		return &PauseConsumersByGroup{
			GroupID: c.PauseConsumersByGroup.GroupId,
			Topics:  c.PauseConsumersByGroup.Topics,
		}, nil
	case *adminpb.AdminCommand_ResumeConsumersByGroup:
		return &ResumeConsumersByGroup{
			GroupID: c.ResumeConsumersByGroup.GroupId,
			Topics:  c.ResumeConsumersByGroup.Topics,
		}, nil
	default:
		return nil, fmt.Errorf("admin: unknown command type %T", cmd.Command)
	}
}
