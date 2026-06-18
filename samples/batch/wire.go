//go:build wireinject
// +build wireinject

package main

import (
	"github.com/google/wire"
	"github.com/xiaoxiaodek/kafkaflow-go/builder"
	kfwire "github.com/xiaoxiaodek/kafkaflow-go/wire"
)

func InitializeApp() (*builder.Bus, func(), error) {
	wire.Build(kfwire.CoreSet)
	return nil, nil, nil
}
