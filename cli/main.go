package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/urfave/cli/v3"
)

func main() {
	cmd := &cli.Command{
		Name:  "rttys-cli",
		Usage: "CLI tool to manage rttys devices",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "server",
				Aliases:  []string{"s"},
				Usage:    "rttys server URL (e.g. http://host:port)",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "password",
				Aliases:  []string{"p"},
				Usage:    "rttys web password",
				Required: true,
			},
		},
		Commands: []*cli.Command{
			{
				Name:   "devices",
				Usage:  "List online devices",
				Action: devicesAction,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "group",
						Aliases: []string{"g"},
						Usage:   "filter by group",
					},
				},
			},
			{
				Name:   "groups",
				Usage:  "List device groups",
				Action: groupsAction,
			},
			{
				Name:   "exec",
				Usage:  "Execute command on a device",
				Action: execAction,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "id",
						Usage:    "device ID",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "cmd",
						Aliases:  []string{"c"},
						Usage:    "command to execute",
						Required: true,
					},
					&cli.StringFlag{
						Name:  "user",
						Usage: "login username",
						Value: "root",
					},
					&cli.StringFlag{
						Name:    "group",
						Aliases: []string{"g"},
						Usage:   "device group",
					},
					&cli.IntFlag{
						Name:  "wait",
						Usage: "wait timeout in seconds (0=no wait)",
						Value: 30,
					},
				},
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

type client struct {
	baseURL  string
	password string
	http     *http.Client
}

func newClient(cmd *cli.Command) (*client, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}

	c := &client{
		baseURL:  strings.TrimRight(cmd.String("server"), "/"),
		password: cmd.String("password"),
		http:     &http.Client{Jar: jar},
	}

	if err := c.signin(); err != nil {
		return nil, fmt.Errorf("signin failed: %w", err)
	}

	return c, nil
}

func (c *client) signin() error {
	body, _ := json.Marshal(map[string]string{"password": c.password})
	resp, err := c.http.Post(c.baseURL+"/signin", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("authentication failed (status %d)", resp.StatusCode)
	}
	return nil
}

func (c *client) get(path string) ([]byte, error) {
	resp, err := c.http.Get(c.baseURL + path)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request failed (status %d)", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func (c *client) postJSON(path string, payload any) ([]byte, error) {
	body, _ := json.Marshal(payload)
	resp, err := c.http.Post(c.baseURL+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func devicesAction(_ context.Context, cmd *cli.Command) error {
	c, err := newClient(cmd)
	if err != nil {
		return err
	}

	group := cmd.String("group")
	data, err := c.get(fmt.Sprintf("/devs?group=%s", group))
	if err != nil {
		return err
	}

	var devs []struct {
		ID        string `json:"id"`
		Group     string `json:"group"`
		Desc      string `json:"description"`
		Connected uint32 `json:"connected"`
		IPaddr    string `json:"ipaddr"`
	}
	if err := json.Unmarshal(data, &devs); err != nil {
		return err
	}

	if len(devs) == 0 {
		fmt.Println("No devices online")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tGROUP\tIP\tDESCRIPTION")
	for _, d := range devs {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", d.ID, d.Group, d.IPaddr, d.Desc)
	}
	return w.Flush()
}

func groupsAction(_ context.Context, cmd *cli.Command) error {
	c, err := newClient(cmd)
	if err != nil {
		return err
	}

	data, err := c.get("/groups")
	if err != nil {
		return err
	}

	var groups []string
	if err := json.Unmarshal(data, &groups); err != nil {
		return err
	}

	for _, g := range groups {
		if g == "" {
			fmt.Println("(ungrouped)")
		} else {
			fmt.Println(g)
		}
	}
	return nil
}

func execAction(_ context.Context, cmd *cli.Command) error {
	c, err := newClient(cmd)
	if err != nil {
		return err
	}

	devid := cmd.String("id")
	group := cmd.String("group")
	wait := cmd.Int("wait")

	payload := map[string]any{
		"cmd":      cmd.String("cmd"),
		"username": cmd.String("user"),
		"params":   []string{},
	}

	path := fmt.Sprintf("/cmd/%s?group=%s&wait=%d", devid, group, wait)
	data, err := c.postJSON(path, payload)
	if err != nil {
		return err
	}

	var result struct {
		Err    int    `json:"err"`
		Msg    string `json:"msg"`
		DevID  string `json:"devid"`
		Code   int    `json:"code"`
		Stdout string `json:"stdout"`
		Stderr string `json:"stderr"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		fmt.Print(string(data))
		return nil
	}

	if result.Err != 0 {
		return fmt.Errorf("command error: %s", result.Msg)
	}

	if result.Stdout != "" {
		decoded, err := base64.StdEncoding.DecodeString(result.Stdout)
		if err != nil {
			fmt.Print(result.Stdout)
		} else {
			fmt.Print(string(decoded))
		}
	}

	if result.Stderr != "" {
		decoded, err := base64.StdEncoding.DecodeString(result.Stderr)
		if err != nil {
			fmt.Fprint(os.Stderr, result.Stderr)
		} else {
			fmt.Fprint(os.Stderr, string(decoded))
		}
	}

	if result.Code != 0 {
		os.Exit(result.Code)
	}
	return nil
}
