# README

This project provides a compatible rest implementation of the rest storage backend.
Implemented storage providers:

- memory
- file
- badgerdb

example:

```go
registryOptions := &options.Options{
		Prefix: configDir,
		Type:   options.StorageType_KV,
		DB:     db,
	}


configStorageProvider := genericregistry.NewStorageProvider(ctx, &config.Config{}, &configregistryOptions)
```