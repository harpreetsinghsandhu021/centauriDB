package embedded

import (
	"centauri/internal/app/govanguard"
	"centauri/internal/app/server"
	"fmt"
)

//	An embedded database driver implementation that adapts
//
// the core govanguard.DriverAdapter interface. It provides functionality for
// interfacing with an embedded database system within the application.
type EmbeddedDriver struct {
	govanguard.DriverAdapter
}

// Establishes a connection to an embedded CentauriDB instance.
// It initializes a new database with the given name and returns an embedded connection wrapper.
// Parameters:
//   - dbName: The name of the database to connect to
//   - properties: A map of connection properties (currently unused)
//
// Returns:
//   - *EmbeddedConnection: A pointer to the established database connection
//   - error: An error if the connection could not be established
func Connect(dbName string, properties map[string]string) (*EmbeddedConnection, error) {
	db, err := server.NewCentauriDB(dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	return NewEmbeddedConnection(db), nil
}
