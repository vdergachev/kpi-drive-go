package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Custom types

type EventTime struct {
	time.Time
}

func (et *EventTime) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	layout := "2006-01-02T15:04:05.999999999Z"
	t, err := time.Parse(layout, raw)
	if err != nil {
		return err
	}

	et.Time = t
	return nil
}

// Api definition

type Response[T any] struct {
	Messages ResponseMessages `json:"MESSAGES"`
	Data     T                `json:"DATA"`
	Status   string           `json:"STATUS"`
}

type ResponseMessages struct {
	Error   []string `json:"error"`
	Warning []string `json:"warning"`
	Info    []string `json:"info"`
}

type ResponseData[T any] struct {
	Page       int `json:"page"`
	PagesCount int `json:"pages_count"`
	RowsCount  int `json:"rows_count"`
	Rows       []T `json:"rows"`
}

type Event struct {
	Author EventAuthor `json:"author"`
	Time   EventTime   `json:"time"`
	Params EventParams `json:"params"`
}

type EventAuthor struct {
	MoId     int    `json:"mo_id"`
	UserId   int    `json:"user_id"`
	UserName string `json:"user_name"`
}

type EventParams struct {
	IndicatorToMoId int               `json:"indicator_to_mo_id"`
	Platform        string            `json:"platform"`
	Period          EventParamsPeriod `json:"period"`
}

type FactSaved struct {
	IndicatorToMoFactId int `json:"indicator_to_mo_fact_id"`
}

type EventParamsPeriod struct {
	End     string `json:"end"`
	Start   string `json:"start"`
	TypeId  int    `json:"type_id"`
	TypeKey string `json:"type_key"`
}

type Fact struct {
	PeriodStart   string
	PeriodEnd     string
	PeriodKey     string
	IndToMoId     string
	IndToMoFactId string
	Value         string
	FactTime      string
	IsPlan        string
	SuperTags     string
	AuthUserId    string
	Comment       string
}

type Tag struct {
	ID           int    `json:"id"`
	Name         string `json:"name"`
	Key          string `json:"key"`
	ValuesSource int    `json:"values_source"`
}

type Data struct {
	Tag   Tag    `json:"tag"`
	Value string `json:"value"`
}

type KpiDriveClient struct {
	baseUrl string
	client  *http.Client
}

func NewKpiDriveClient(baseUrl string) *KpiDriveClient {
	jar, _ := cookiejar.New(nil)
	client := &http.Client{
		Jar: jar,
	}
	return &KpiDriveClient{
		baseUrl: baseUrl,
		client:  client,
	}
}

func (c KpiDriveClient) Auth(login string, password string) error {
	authPayload := strings.NewReader(fmt.Sprintf("login=%s&password=%s", login, password))

	authReq, err := http.NewRequest("POST", c.baseUrl+"/_api/auth/login", authPayload)
	if err != nil {
		fmt.Println(err)
		return err
	}

	authReq.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	authRes, err := c.client.Do(authReq)
	if err != nil {
		return err
	}
	defer authRes.Body.Close()

	authBody, err := io.ReadAll(authRes.Body)
	if err != nil {
		return err
	}

	println("auth result: " + string(authBody))
	return nil
}

func (c KpiDriveClient) GetEvents(out *Response[ResponseData[Event]]) error {
	filter := bytes.NewReader([]byte(`{
		"filter": {
			"field": {
				"key": "type",
				"sign": "LIKE",
				"values": [
					"MATRIX_REQUEST"
				]
			}
		},
		"sort": {
			"fields": [
				"time"
			],
			"direction": "DESC"
		},
		"limit": 10
	}`))

	req, err := http.NewRequest("GET", c.baseUrl+"/_api/events", filter)
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")

	res, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	// Read the JSON response
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	// Parse
	return json.Unmarshal(body, out)
}

func (c KpiDriveClient) SaveFact(token string, fact *Fact, out *Response[FactSaved]) error {

	data := url.Values{}
	data.Add("period_start", fact.PeriodStart)
	data.Add("period_end", fact.PeriodEnd)
	data.Add("period_key", fact.PeriodKey)
	data.Add("indicator_to_mo_id", fact.IndToMoId)
	data.Add("indicator_to_mo_fact_id", fact.IndToMoFactId)
	data.Add("value", fact.Value)
	data.Add("fact_time", fact.FactTime)
	data.Add("is_plan", fact.IsPlan)
	data.Add("supertags", fact.SuperTags)
	data.Add("auth_user_id", fact.AuthUserId)
	data.Add("comment", fact.Comment)

	encData := data.Encode()
	body := strings.NewReader(encData)

	req, err := http.NewRequest("POST", c.baseUrl+"/_api/facts/save_fact", body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(encData)))
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(respBody, out)
}

func main() {
	token := "48ab34464a5573519725deb5865cc74c"
	client := NewKpiDriveClient("https://development.kpi-drive.ru")

	if err := client.Auth("admin", "admin"); err != nil {
		fmt.Println(err)
		return
	}
	events := Response[ResponseData[Event]]{}

	if err := client.GetEvents(&events); err != nil {
		fmt.Println(err)
		return
	}

	for _, event := range events.Data.Rows {

		fact := Fact{
			PeriodKey:     "month",
			AuthUserId:    "40",
			Value:         "1",
			IndToMoFactId: "0",
			IndToMoId:     "315914",
			Comment:       "ArangoDB",
		}

		populateFact(event, &fact)

		factSaved := Response[FactSaved]{}
		if err := client.SaveFact(token, &fact, &factSaved); err != nil {
			fmt.Println(err)
			break
		}

		if factSaved.Status == "OK" {
			fmt.Println("Fact saved, id = " + strconv.Itoa(factSaved.Data.IndicatorToMoFactId))
		} else {
			fmt.Println("Fact save failure, error = " + factSaved.Messages.Error[0])
			break
		}
	}
}

func populateFact(event Event, fact *Fact) {
	tagsData := Data{
		Tag: Tag{
			ID:           event.Author.UserId,
			Name:         "Клиент",
			Key:          "client",
			ValuesSource: 0,
		},
		Value: event.Author.UserName,
	}

	tags, _ := json.Marshal([]Data{tagsData})

	fact.PeriodStart = event.Params.Period.Start
	fact.PeriodEnd = event.Params.Period.End
	fact.FactTime = event.Time.Format("2006-01-02")
	fact.IndToMoFactId = strconv.Itoa(event.Params.IndicatorToMoId)
	fact.SuperTags = string(tags)
}
