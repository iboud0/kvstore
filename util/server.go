package util

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// Server represents the key-value store server.
type Server struct {
	Router *mux.Router
	db     *MemDB
}

// NewServer creates a new instance of the server.
func NewServer() (*Server, error) {
	mem, err := NewMemDB()
	if err != nil {
		return nil, err
	}

	return &Server{
		Router: mux.NewRouter(),
		db:     mem,
	}, nil
}

// SetupRoutes configures the server routes.
func (s *Server) SetupRoutes() {
	s.Router.HandleFunc("/get", s.GetHandler).Methods("GET")
	s.Router.HandleFunc("/set", s.SetHandler).Methods("POST")
	s.Router.HandleFunc("/del", s.DeleteHandler).Methods("DELETE")
}

// GetHandler handles GET requests and retrieves the value for a given key.
func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Key not provided", http.StatusBadRequest)
		return
	}

	value, err := s.db.Get([]byte(key))
	if err != nil {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(value)
}

// SetHandler handles POST requests and inserts a key-value pair into the MemTable.
func (s *Server) SetHandler(w http.ResponseWriter, r *http.Request) {
	var data map[string]string

	// Use json.NewDecoder directly to decode the JSON payload from the request body
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "Error decoding JSON", http.StatusBadRequest)
		return
	}

	key, ok := data["key"]
	if !ok || key == "" {
		http.Error(w, "Invalid or missing 'key' in JSON", http.StatusBadRequest)
		return
	}

	value, ok := data["value"]
	if !ok {
		http.Error(w, "Invalid or missing 'value' in JSON", http.StatusBadRequest)
		return
	}

	s.db.Set([]byte(key), []byte(value))

	w.WriteHeader(http.StatusCreated)
}

// DeleteHandler handles DELETE requests and deletes a key from the MemTable, returning the existing value.
func (s *Server) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Key not provided", http.StatusBadRequest)
		return
	}

	existingValue, err := s.db.Get([]byte(key))
	if err != nil {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	s.db.Del([]byte(key))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(existingValue)
}
