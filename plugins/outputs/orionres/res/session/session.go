package session

import (
	"encoding/xml"
	"net/http"

	"github.com/SpirentOrion/logrus"
	"github.com/SpirentOrion/luddite"
	"github.com/influxdata/telegraf/plugins/outputs/orionres/processor"
)

const (
	EcodeClientConnect = "CLIENT_CONNECT"
)

var errorDefs = map[string]string{
	EcodeClientConnect: "Error creating orionres connection: %s",
}

type Session struct {
	XMLName xml.Name `json:"-" xml:"session"`
	DbId    string   `json:"db_id", xml:"db_id"`
	Url     string   `json:"url,omitempty" xml:"url,omitempty"`
	TestKey string   `json:"test_key,omitempty" xml:"test_key,omitempty"`
}

func newSession(c *processor.TestClient) *Session {
	return &Session{
		DbId:    c.DbId,
		Url:     c.Url,
		TestKey: c.TestKey,
	}
}

type sessionResource struct {
	processor *processor.Processor
}

func NewResource(p *processor.Processor) *sessionResource {
	return &sessionResource{processor: p}
}

func (r *sessionResource) New() interface{} {
	return &Session{}
}

func (r *sessionResource) Id(value interface{}) string {
	u := value.(*Session)
	return u.DbId
}

func (r *sessionResource) List(req *http.Request) (int, interface{}) {
	var l []Session
	if c := r.processor.Client(); c != nil {
		l = append(l, *newSession(c))
	}

	return http.StatusOK, l
}

func (r *sessionResource) Count(req *http.Request) (int, interface{}) {
	var count int
	if r.processor.Client() != nil {
		count++
	}
	return http.StatusOK, 0
}

func (r *sessionResource) Get(req *http.Request, id string) (int, interface{}) {
	c := r.processor.Client()
	if c == nil || c.DbId != id {
		return http.StatusNotFound, nil
	}

	return http.StatusOK, *newSession(c)
}

func (r *sessionResource) Create(req *http.Request, value interface{}) (int, interface{}) {
	s := value.(*Session)

	c, err := processor.NewClient(s.Url, s.DbId, "", s.TestKey)
	if err != nil {
		return http.StatusBadRequest, luddite.NewError(errorDefs, EcodeClientConnect, err)
	}
	r.processor.SetClient(c)

	logger := luddite.ContextLogger(req.Context())
	logger.WithFields(logrus.Fields{
		"event": "Created",
		"name":  s.DbId,
	}).Info()
	return http.StatusCreated, s
}

func (r *sessionResource) Delete(req *http.Request, id string) (int, interface{}) {
	c := r.processor.Client()
	if c == nil || c.DbId != id {
		return http.StatusNotFound, nil
	}
	r.processor.SetClient(nil)
	logger := luddite.ContextLogger(req.Context())
	logger.WithFields(logrus.Fields{
		"event": "Deleted",
		"name":  id,
	}).Info()
	return http.StatusNoContent, nil
}

func (r *sessionResource) Update(req *http.Request, id string, value interface{}) (int, interface{}) {
	return http.StatusNotFound, nil
}

func (r *sessionResource) Action(req *http.Request, id string, action string) (int, interface{}) {
	return http.StatusNotFound, nil
}
