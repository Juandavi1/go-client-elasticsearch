package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

var (
	host      string = os.Getenv("host_elastic")
	index     string = os.Getenv("index_elastic")
	max       int    = 10000
	ZERO      int  = 0
	UNO       int    = 1
	gorutines int    = 2
	group     sync.WaitGroup
)

/* --- structs --- */

type Record struct {
	Message    string `json:"message"`
	RequestId  string `json:"requestId"`
	RequestId2 string `json:"RequestId"`
}

type UpdatePriceInfoApi struct {
	TotalFare       float64       `json:"totalFare"`
	TotalTax        float64       `json:"totalTax"`
	BaseFare        float64       `json:"baseFare"`
	PtcTotal        float64       `json:"ptcTotal"`
	TotalSurcharges float64       `json:"totalSurcharges"`
	Quantity        int64         `json:"quantity"`
	FareType        string        `json:"fareType"`
	PassengerType   string        `json:"passengerType"`
	CorporateIds    []interface{} `json:"corporateIds"`
	ExtraTaxes      []struct {
		Amount float64 `json:"amount"`
		Type   string  `json:"type"`
	} `json:"extraTaxes"`
	Surcharges []interface{} `json:"surcharges"`
}

type ResponseConnector struct {
	Segments []struct {
		Legs []struct {
			Origin struct {
				Code        string `json:"code"`
				Name        string `json:"name"`
				CountryName string `json:"countryName"`
				CityName    string `json:"cityName"`
				Timezone    string `json:"timezone"`
			} `json:"origin"`
			Destination struct {
				Code        string `json:"code"`
				Name        string `json:"name"`
				CountryName string `json:"countryName"`
				CityName    string `json:"cityName"`
				Timezone    string `json:"timezone"`
			} `json:"destination"`
			DepartureDate    string `json:"departureDate"`
			DepartureTime    string `json:"departureTime"`
			ArrivalDate      string `json:"arrivalDate"`
			ArrivalTime      string `json:"arrivalTime"`
			MarketingCarrier struct {
				Code string `json:"code"`
				Name string `json:"name"`
			} `json:"marketingCarrier"`
			OperatingCarrier struct {
				Code string `json:"code"`
				Name string `json:"name"`
			} `json:"operatingCarrier"`
			Number   int `json:"number"`
			Aircraft struct {
				Code string `json:"code"`
				Name string `json:"name"`
			} `json:"aircraft"`
			Cabin struct {
				Designator  string `json:"designator"`
				Code        string `json:"code"`
				Description string `json:"description"`
			} `json:"cabin"`
			FareBase       string `json:"fareBase"`
			FlightDuration string `json:"flightDuration"`
		} `json:"legs"`
		BaggageMessage string `json:"baggageMessage"`
	} `json:"segments"`
	PriceInfo struct {
		Adults         UpdatePriceInfoApi `json:"adults"`
		Children       UpdatePriceInfoApi `json:"children"`
		Infants        UpdatePriceInfoApi `json:"infants"`
		Currency       string             `json:"currency"`
		ConversionRate float64            `json:"conversionRate"`
	} `json:"priceInfo"`
	UpdatedPriceInfo struct {
		Adults struct {
			Fare struct {
				BaseFare  float64 `json:"baseFare"`
				NetFare   float64 `json:"netFare"`
				TotalFare float64 `json:"totalFare"`
				Detail    struct {
					CommissionAndOver float32 `json:"commissionAndOver"`
					Markup            float32 `json:"markup"`
				} `json:"detail"`
			} `json:"fare"`
			TotalFare float64 `json:"totalFare"`
			TotalTax  float64 `json:"totalTax"`
			PtcTotal  float64 `json:"ptcTotal"`
			Quantity  int     `json:"quantity"`
		} `json:"adults"`
	} `json:"updatedPriceInfo"`
	ValidatingCarrier string `json:"validatingCarrier"`
	Provider          string `json:"provider"`
	FlightType        string `json:"flightType"`
	Domestic          bool   `json:"domestic"`
	CountryCode       string `json:"countryCode"`
	IsCancellable     bool   `json:"isCancellable"`
}

type ElasticClient struct {
	client *elastic.Client
}

/* --- end structs  ---   */

func init() {
	group.Add(gorutines)
}

func main() {

	requesId := make([]Record, ZERO)
	validations := make([]Record, ZERO)
	messages := make([]string, ZERO)

	conection := newClient()
	defer conection.client.Stop()

	go func() {
		defer group.Done()
		query := "appname:api-promocodes AND category:LOG_RESPONSE_BODY AND " +
			"requestUri:/api/promocodes/promocodes/promo-codes-amounts AND NOT " +
			"status:200 AND promocodeId:UNIVERSALDIC AND errorCode:40903001"

		requesId = conection.search(query)
	}()

	go func() {
		defer group.Done()
		query := "appname:api-promocodes AND category:VALIDATE_PROMOCODE_APPLY AND" +
			" logger_name:com.almundo.promocodes.service.ValidationPromocodeService " +
			"AND promocodeId:UNIVERSALDIC"

		validations = conection.search(query)
	}()

	group.Wait()

	for _, validation := range validations {
		for _, request := range requesId {
			if strings.Compare(validation.RequestId, request.RequestId) == 0 {
				messages = append(messages, validation.Message)
				break
			}
		}
	}

	// map response to export csv
	var data [][]string
	headers := []string{
		"Pais", "Tipo", "validatingCarrier", "Precio", "Moneda", "Adultos", "Infantes", "Niños", "Origen", "Codigo Origen", "Fecha Salida",
		"Destino", "Codigo Destino", "Fecha Llegaga", "Origen Regreso", "Codigo Origen Regreso", "Fecha Salida Regreso",
		"Destino Regreso", "Codigo Destino Regreso", "Fecha LLegada Regreso",
	}
	data = append(data, headers)

	for _, message := range messages {
		var response ResponseConnector
		err := json.Unmarshal([]byte(message), &response)
		if err != nil {
			log.Fatal(err)
		}

		item := make([]string, ZERO)
		item = append(item, response.CountryCode)
		item = append(item, response.FlightType)
		item = append(item, response.ValidatingCarrier)
		item = append(item, fmt.Sprintf("%.1f", response.PriceInfo.Adults.TotalFare))
		item = append(item, response.PriceInfo.Currency)
		item = append(item, getQuanty(&response.PriceInfo.Adults))
		item = append(item, getQuanty(&response.PriceInfo.Infants))
		item = append(item, getQuanty(&response.PriceInfo.Children))

		item = append(item, response.Segments[ZERO].Legs[ZERO].Origin.CityName)
		item = append(item, response.Segments[ZERO].Legs[ZERO].Origin.Code)
		item = append(item, response.Segments[ZERO].Legs[ZERO].DepartureDate)
		item = append(item, response.Segments[ZERO].Legs[ZERO].Destination.CityName)
		item = append(item, response.Segments[ZERO].Legs[ZERO].Destination.Code)
		item = append(item, response.Segments[ZERO].Legs[ZERO].ArrivalDate)

		if len(response.Segments) == 2 {
			leg := len(response.Segments[UNO].Legs) - UNO
			item = append(item, response.Segments[UNO].Legs[leg].Origin.CityName)
			item = append(item, response.Segments[UNO].Legs[leg].Origin.Code)
			item = append(item, response.Segments[UNO].Legs[leg].DepartureDate)
			item = append(item, response.Segments[UNO].Legs[leg].Destination.CityName)
			item = append(item, response.Segments[UNO].Legs[leg].Destination.Code)
			item = append(item, response.Segments[UNO].Legs[leg].ArrivalDate)
		}

		data = append(data, item)

	}

	exportCsv(data)

}

func newClient() *ElasticClient {

	client_, err := elastic.NewClient(elastic.SetURL(host))
	if err != nil {
		log.Fatal(err)
	}
	return &ElasticClient{
		client: client_,
	}
}

func (e ElasticClient) search(query string) (record []Record) {

	termQuery := elastic.NewQueryStringQuery(query)

	searchResult, err := e.client.
		Search().
		Index(index).
		Query(termQuery).
		From(ZERO).
		Size(max).
		Do(context.Background())

	if err != nil {
		log.Fatal(err)
	}

	if searchResult.Hits.TotalHits > 0 {

		for _, hit := range searchResult.Hits.Hits {
			var v Record
			err := json.Unmarshal(*hit.Source, &v)
			if err != nil {
				print(err)
			}
			if len(v.Message) != 0 {
				record = append(record, v)
			}
		}

	} else {
		fmt.Print("not found promos\n")
	}

	return
}

func getQuanty(obj *UpdatePriceInfoApi) (value string) {
	return strconv.FormatInt(obj.Quantity, 10)
}

func exportCsv(records [][]string) () {
	file, err := os.Create("result.csv")
	checkError("Cannot create file", err)
	defer file.Close()

	w := csv.NewWriter(file)
	defer w.Flush()
	for _, record := range records {
		if err := w.Write(record); err != nil {
			log.Fatalln("error writing record to csv:", err)
		}
	}
	if err := w.Error(); err != nil {
		log.Fatal(err)
	}
}

func checkError(message string, err error) () {
	if err != nil {
		log.Fatal(message, err)
	}
}
