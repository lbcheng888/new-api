package channel

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	common2 "one-api/common" // Added for ChannelTypeGemini
	"one-api/relay/common"
	relayconstant "one-api/relay/constant"
	"one-api/service"
	"os"
	"sync" // Added for Mutex
	"time" // Added for time.Minute

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// Global cache for Gemini Key Managers
var (
	geminiKeyManagers   = make(map[int]*service.KeyManager)
	geminiKeyManagersMu sync.Mutex
)

func SetupApiRequestHeader(info *common.RelayInfo, c *gin.Context, req *http.Header) {
	// Use the correct package alias 'relayconstant' for RelayMode* constants
	if info.RelayMode == relayconstant.RelayModeAudioTranscription || info.RelayMode == relayconstant.RelayModeAudioTranslation {
		// multipart/form-data
	} else if info.RelayMode == relayconstant.RelayModeRealtime {
		// websocket
	} else {
		req.Set("Content-Type", c.Request.Header.Get("Content-Type"))
		req.Set("Accept", c.Request.Header.Get("Accept"))
		if info.IsStream && c.Request.Header.Get("Accept") == "" {
			req.Set("Accept", "text/event-stream")
		}
	}
}

func DoApiRequest(a Adaptor, c *gin.Context, info *common.RelayInfo, requestBody io.Reader) (*http.Response, error) {
	fullRequestURL, err := a.GetRequestURL(info)
	if err != nil {
		return nil, fmt.Errorf("get request url failed: %w", err)
	}
	if common2.DebugEnabled {
		println("fullRequestURL:", fullRequestURL)
	}
	req, err := http.NewRequest(c.Request.Method, fullRequestURL, requestBody)
	if err != nil {
		return nil, fmt.Errorf("new request failed: %w", err)
	}
	var selectedKey string
	var keyManager *service.KeyManager
	isGemini := info.ChannelType == common2.ChannelTypeGemini

	if isGemini {
		geminiKeyManagersMu.Lock()
		manager, exists := geminiKeyManagers[info.ChannelId]
		if !exists {
			if os.Getenv("GEMINI_API_KEYS") != "" {
				info.ApiKey = os.Getenv("GEMINI_API_KEYS")
			}
			manager = service.NewKeyManager(info.ApiKey, 5, 1*time.Minute)
			geminiKeyManagers[info.ChannelId] = manager
		}
		keyManager = manager
		geminiKeyManagersMu.Unlock()

		var keyErr error
		selectedKey, keyErr = keyManager.GetAvailableKey(c.Request.Context())
		if keyErr != nil {
			if errors.Is(keyErr, service.ErrNoAvailableKey) {
				openAIError := service.OpenAIErrorWrapper(keyErr, "rate_limit_exceeded", http.StatusTooManyRequests)
				return nil, fmt.Errorf("rate limit exceeded: %s", openAIError.Error.Message)
			}
			common2.LogError(c.Request.Context(), fmt.Errorf("get available key failed: %w", keyErr).Error())
			return nil, fmt.Errorf("get available key failed: %w", keyErr)
		}
		c.Set(common2.RelayApiKey, selectedKey)
	}

	err = a.SetupRequestHeader(c, &req.Header, info)
	if err != nil {
		return nil, fmt.Errorf("setup request header failed: %w", err)
	}

	// --- Gemini Key Pool Header Override ---
	if isGemini {
		// Explicitly set the selected key for Gemini, overriding any key set by the adaptor
		req.Header.Set("x-goog-api-key", selectedKey)
	}
	// --- Gemini Key Pool Header Override End ---

	// Log before attempting the request
	resp, err := doRequest(c, req, info)
	// Log after the request attempt
	if err != nil {
		common2.LogError(c.Request.Context(), fmt.Sprintf("doRequest failed: %s", err.Error()))
		// Note: err is already being returned below, just logging here.
	} else if resp != nil {
		common2.LogInfo(c.Request.Context(), fmt.Sprintf("Received response with status code: %d, apikey: %s", resp.StatusCode, selectedKey))
	} else {
		common2.LogWarn(c.Request.Context(), "doRequest returned nil response and nil error")
	}

	if err != nil {
		return nil, fmt.Errorf("do request failed: %w", err)
	}
	return resp, nil
}

func DoFormRequest(a Adaptor, c *gin.Context, info *common.RelayInfo, requestBody io.Reader) (*http.Response, error) {
	fullRequestURL, err := a.GetRequestURL(info)
	if err != nil {
		return nil, fmt.Errorf("get request url failed: %w", err)
	}
	req, err := http.NewRequest(c.Request.Method, fullRequestURL, requestBody)
	if err != nil {
		return nil, fmt.Errorf("new request failed: %w", err)
	}
	// set form data
	req.Header.Set("Content-Type", c.Request.Header.Get("Content-Type"))

	err = a.SetupRequestHeader(c, &req.Header, info)
	if err != nil {
		return nil, fmt.Errorf("setup request header failed: %w", err)
	}
	resp, err := doRequest(c, req, info)
	if err != nil {
		return nil, fmt.Errorf("do request failed: %w", err)
	}
	return resp, nil
}

func DoWssRequest(a Adaptor, c *gin.Context, info *common.RelayInfo, requestBody io.Reader) (*websocket.Conn, error) {
	fullRequestURL, err := a.GetRequestURL(info)
	if err != nil {
		return nil, fmt.Errorf("get request url failed: %w", err)
	}
	targetHeader := http.Header{}
	err = a.SetupRequestHeader(c, &targetHeader, info)
	if err != nil {
		return nil, fmt.Errorf("setup request header failed: %w", err)
	}
	targetHeader.Set("Content-Type", c.Request.Header.Get("Content-Type"))
	targetConn, _, err := websocket.DefaultDialer.Dial(fullRequestURL, targetHeader)
	if err != nil {
		return nil, fmt.Errorf("dial failed to %s: %w", fullRequestURL, err)
	}
	// send request body
	//all, err := io.ReadAll(requestBody)
	//err = service.WssString(c, targetConn, string(all))
	return targetConn, nil
}

func doRequest(c *gin.Context, req *http.Request, info *common.RelayInfo) (*http.Response, error) {
	var client *http.Client
	var err error
	if proxyURL, ok := info.ChannelSetting["proxy"]; ok {
		client, err = service.NewProxyHttpClient(proxyURL.(string))
		if err != nil {
			return nil, fmt.Errorf("new proxy http client failed: %w", err)
		}
	} else {
		client = service.GetHttpClient()
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errors.New("resp is nil")
	}
	_ = req.Body.Close()
	_ = c.Request.Body.Close()
	return resp, nil
}

func DoTaskApiRequest(a TaskAdaptor, c *gin.Context, info *common.TaskRelayInfo, requestBody io.Reader) (*http.Response, error) {
	fullRequestURL, err := a.BuildRequestURL(info)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(c.Request.Method, fullRequestURL, requestBody)
	if err != nil {
		return nil, fmt.Errorf("new request failed: %w", err)
	}
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(requestBody), nil
	}

	err = a.BuildRequestHeader(c, req, info)
	if err != nil {
		return nil, fmt.Errorf("setup request header failed: %w", err)
	}
	resp, err := doRequest(c, req, info.RelayInfo)
	if err != nil {
		return nil, fmt.Errorf("do request failed: %w", err)
	}
	return resp, nil
}
