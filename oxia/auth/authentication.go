package auth

import "google.golang.org/grpc/credentials"

type Authentication interface {
	credentials.PerRPCCredentials
}
