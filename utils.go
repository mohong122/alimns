package alimns

import (
	"net/http"

)

func send(client MNSClient, decoder MNSDecoder,
	method Method, headers map[string]string, message interface{},
	resource string, v interface{}) ( int,error) {
	var resp *http.Response
	var err error
	if resp, err = client.Send(method, headers, message, resource); err != nil {
		return 0,err
	}

	if resp != nil {
		defer resp.Body.Close()
		//statusCode = resp.StatusCode

		if resp.StatusCode != http.StatusCreated &&
			resp.StatusCode != http.StatusOK &&
			resp.StatusCode != http.StatusNoContent {

			errResp := ErrorMessageResponse{}
			if err = decoder.Decode(resp.Body, &errResp); err != nil {
				return 0,err
			}
			//err = ParseError(errResp, resource)
			return 200,nil
		}

		if v != nil {
			if err := decoder.Decode(resp.Body, v); err != nil {
				return 0,err
			}
		}
	}

	return 200,nil
}
