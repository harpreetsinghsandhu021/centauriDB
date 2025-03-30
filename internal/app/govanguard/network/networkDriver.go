package network

// Implements the standard Go sql.Driver interface by
// wrapping our RemoteDriver
type NetworkDriver struct {
}

// Returns a new connection to the database
// func (d *NetworkDriver) Connect(dataSourceName string) (driver.Conn, error) {
// 	// Parse the host from the connection string
// 	host := strings.Replace(dataSourceName, "centauridb://", "", 1)
// 	// Remove the port if there is one
// 	host = strings.Split(host, ":")[0]

// 	// Connect to the rpc server on port 1099
// 	client, err := jsonrpc.Dial("tcp", host+":1099")
// 	if err != nil {
// 		return nil, err
// 	}

// 	var remoteDriver RemoteDriver
// 	// Creates an instance of remoteDriverServer
// 	remoteDriver = new(DriverServer)

// 	err = client.Call("centauridb", nil, &remoteDriver)

// }
