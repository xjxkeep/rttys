package main

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/urfave/cli/v3"
)

func main() {
	cmd := &cli.Command{
		Name:  "rttys-cli",
		Usage: "CLI tool to manage rttys devices",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "server",
				Aliases: []string{"s"},
				Usage:   "rttys server URL (e.g. http://host:port)",
			},
			&cli.StringFlag{
				Name:    "password",
				Aliases: []string{"p"},
				Usage:   "rttys web password",
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
					&cli.StringSliceFlag{
						Name:    "arg",
						Aliases: []string{"a"},
						Usage:   "argument passed to the executable (repeatable)",
					},
					&cli.BoolFlag{
						Name:  "shell",
						Usage: "run --cmd through /bin/sh -lc",
					},
				},
			},
			{
				Name:   "upload",
				Usage:  "Upload a local file to device",
				Action: uploadAction,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "id",
						Usage:    "device ID",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "file",
						Aliases:  []string{"f"},
						Usage:    "local file path to upload",
						Required: true,
					},
					&cli.StringFlag{
						Name:    "group",
						Aliases: []string{"g"},
						Usage:   "device group",
					},
				},
			},
			{
				Name:   "download",
				Usage:  "Download a file from device",
				Action: downloadAction,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "id",
						Usage:    "device ID",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "remote",
						Aliases:  []string{"r"},
						Usage:    "remote file path on device",
						Required: true,
					},
					&cli.StringFlag{
						Name:    "output",
						Aliases: []string{"o"},
						Usage:   "local output file path (default: same filename in current dir)",
					},
					&cli.StringFlag{
						Name:    "group",
						Aliases: []string{"g"},
						Usage:   "device group",
					},
				},
			},
			{
				Name:   "mcp",
				Usage:  "Run as MCP server (stdio transport)",
				Action: mcpAction,
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// --- CLI Actions ---

func getServerPassword(cmd *cli.Command) (string, string, error) {
	server := cmd.String("server")
	password := cmd.String("password")
	if server == "" || password == "" {
		return "", "", fmt.Errorf("--server and --password are required")
	}
	return server, password, nil
}

func devicesAction(_ context.Context, cmd *cli.Command) error {
	server, password, err := getServerPassword(cmd)
	if err != nil {
		return err
	}

	devs, err := listDevices(server, password, cmd.String("group"))
	if err != nil {
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
	server, password, err := getServerPassword(cmd)
	if err != nil {
		return err
	}

	groups, err := listGroups(server, password)
	if err != nil {
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
	server, password, err := getServerPassword(cmd)
	if err != nil {
		return err
	}

	command := cmd.String("cmd")
	params := cmd.StringSlice("arg")
	if cmd.Bool("shell") {
		if len(params) > 0 {
			return fmt.Errorf("--arg cannot be combined with --shell")
		}
		params = []string{"-lc", command}
		command = "/bin/sh"
	}

	result, err := execCommandArgs(server, password, cmd.String("id"), cmd.String("group"),
		command, cmd.String("user"), params, int(cmd.Int("wait")))
	if err != nil {
		return err
	}

	if result.Stdout != "" {
		fmt.Print(result.Stdout)
	}
	if result.Stderr != "" {
		fmt.Fprint(os.Stderr, result.Stderr)
	}
	if result.Code != 0 {
		os.Exit(result.Code)
	}
	return nil
}

func uploadAction(_ context.Context, cmd *cli.Command) error {
	server, password, err := getServerPassword(cmd)
	if err != nil {
		return err
	}

	filePath := cmd.String("file")
	sent, err := uploadFile(server, password, cmd.String("id"), cmd.String("group"), filePath)
	if err != nil {
		return err
	}

	fmt.Printf("uploaded %s (%d bytes)\n", filePath, sent)
	return nil
}

func downloadAction(_ context.Context, cmd *cli.Command) error {
	server, password, err := getServerPassword(cmd)
	if err != nil {
		return err
	}

	filename, received, err := downloadFile(server, password, cmd.String("id"), cmd.String("group"),
		cmd.String("remote"), cmd.String("output"))
	if err != nil {
		return err
	}

	fmt.Printf("downloaded %s (%d bytes)\n", filename, received)
	return nil
}
