package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type restClient struct {
	host   string
	client *http.Client
}

func NewRESTClient(clientID string) (twopc.Client, error) {
	return restClient{
		host:   clientID,
		client: &http.Client{},
	}, nil
}

func (c restClient) PrepareTransaction(ctx context.Context, transactionID string, payload twopc.PreparePayload) (err error) {
	var prepareURL string
	prepareURL, err = c.prepareURL(transactionID)
	if err != nil {
		return err
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling payload %v: %w", payload, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, prepareURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("creating req with ctx: %w", err)
	}

	var resp *http.Response
	resp, err = c.client.Do(req)
	if err != nil {
		return fmt.Errorf("sending req %s: %w", prepareURL, err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			closeErr = fmt.Errorf("closing resp: %w", closeErr)
			if err != nil {
				err = errors.Join(err, closeErr)
			} else {
				err = closeErr
			}
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("preparing tx %s failed", transactionID)
	}

	return nil
}

func (c restClient) prepareURL(transactionID string) (string, error) {
	return c.operationURL(transactionID, "prepare")
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
		return fmt.Errorf("committing tx %s failed", transactionID)
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
		return fmt.Errorf("rolling back tx %s failed", transactionID)
	}

	return nil
}

func (c restClient) sendNoBodyOperation(ctx context.Context, operationURL string) (resp *http.Response, err error) {
	var req *http.Request
	req, err = http.NewRequestWithContext(ctx, http.MethodPost, operationURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating req with ctx: %w", err)
	}

	resp, err = c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending req %s: %w", operationURL, err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			closeErr = fmt.Errorf("closing resp: %w", err)
			if err != nil {
				err = errors.Join(err, closeErr)
			} else {
				err = closeErr
			}
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
		return "", fmt.Errorf("parsing host %s: %w", c.host, err)
	}
	base.Path = "/transactions/" + url.PathEscape(transactionID) + "/" + operation
	return base.String(), nil
}
