package kafkaflow

// Resolver is the dependency injection interface. It resolves types registered
// in the DI container (backed by google/wire).
type Resolver interface {
	Resolve(target interface{}) error
}
