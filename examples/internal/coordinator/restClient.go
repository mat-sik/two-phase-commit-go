package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type restClient struct {
	host   string
	client *http.Client
}

func newRESTClient(clientID string) (twopc.Client, error) {
	return restClient{
		host:   clientID,
		client: &http.Client{},
	}, nil
}

func (c restClient) PrepareTransaction(ctx context.Context, transactionID string, payload twopc.PreparePayload) (err error) {
	payload, ok := payload.(RESTPreparePayload)
	if !ok {
		return errors.New("invalid type provided in the request")
	}

	var prepareURL string
	prepareURL, err = c.prepareURL(transactionID)
	if err != nil {
		return err
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, prepareURL, bytes.NewReader(data))
	if err != nil {
		return err
	}

	var resp *http.Response
	resp, err = c.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := resp.Body.Close(); err != nil {
			err = errors.Join(err, closeErr)
		} else {
			err = closeErr
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return errors.New("failed to prepare")
	}

	return nil
}

func (c restClient) prepareURL(transactionID string) (string, error) {
	return c.operationURL(transactionID, "prepare")
}

type RESTPreparePayload struct {
	Payload   string    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}

func (c restClient) CommitTransaction(ctx context.Context, transactionID string) error {
	commitURL, err := c.commitURL(transactionID)
	if err != nil {
		return err
	}

	resp, err := c.sendNoBodyOperation(ctx, commitURL)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		err = errors.New("failed to commit")
		return err
	}

	return nil
}

func (c restClient) commitURL(transactionID string) (string, error) {
	return c.operationURL(transactionID, "commit")
}

func (c restClient) RollbackTransaction(ctx context.Context, transactionID string) error {
	rollbackURL, err := c.rollbackURL(transactionID)
	if err != nil {
		return err
	}

	var resp *http.Response
	resp, err = c.sendNoBodyOperation(ctx, rollbackURL)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		err = errors.New("failed to rollback")
		return err
	}

	return nil
}

func (c restClient) sendNoBodyOperation(ctx context.Context, operationURL string) (resp *http.Response, err error) {
	var req *http.Request
	req, err = http.NewRequestWithContext(ctx, http.MethodPost, operationURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err = c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		closeErr := resp.Body.Close()
		if err != nil {
			err = errors.Join(err, closeErr)
		} else {
			err = closeErr
		}
	}()

	return resp, nil
}

func (c restClient) rollbackURL(transactionID string) (string, error) {
	return c.operationURL(transactionID, "rollback")
}

func (c restClient) operationURL(transactionID string, operation string) (string, error) {
	base, err := url.Parse("http://" + c.host)
	if err != nil {
		return "", err
	}
	base.Path = "/transactions/" + url.PathEscape(transactionID) + "/" + operation
	return base.String(), nil
}
