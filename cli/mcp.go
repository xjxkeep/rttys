package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/urfave/cli/v3"
)

func mcpAction(_ context.Context, cmd *cli.Command) error {
	s := server.NewMCPServer("rttys", "1.0.0")

	// list_devices
	s.AddTool(
		mcp.NewTool("list_devices",
			mcp.WithDescription("列出在线设备"),
			mcp.WithString("server", mcp.Description("rttys 服务器地址"), mcp.Required()),
			mcp.WithString("password", mcp.Description("rttys 登录密码"), mcp.Required()),
			mcp.WithString("group", mcp.Description("按分组筛选")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			srv := req.GetString("server", "")
			pwd := req.GetString("password", "")
			group := req.GetString("group", "")

			devs, err := listDevices(srv, pwd, group)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}

			data, _ := json.MarshalIndent(devs, "", "  ")
			return mcp.NewToolResultText(string(data)), nil
		},
	)

	// list_groups
	s.AddTool(
		mcp.NewTool("list_groups",
			mcp.WithDescription("列出设备分组"),
			mcp.WithString("server", mcp.Description("rttys 服务器地址"), mcp.Required()),
			mcp.WithString("password", mcp.Description("rttys 登录密码"), mcp.Required()),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			srv := req.GetString("server", "")
			pwd := req.GetString("password", "")

			groups, err := listGroups(srv, pwd)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}

			data, _ := json.MarshalIndent(groups, "", "  ")
			return mcp.NewToolResultText(string(data)), nil
		},
	)

	// exec_command
	s.AddTool(
		mcp.NewTool("exec_command",
			mcp.WithDescription("在远程设备上执行命令"),
			mcp.WithString("server", mcp.Description("rttys 服务器地址"), mcp.Required()),
			mcp.WithString("password", mcp.Description("rttys 登录密码"), mcp.Required()),
			mcp.WithString("device_id", mcp.Description("设备 ID"), mcp.Required()),
			mcp.WithString("command", mcp.Description("要执行的命令"), mcp.Required()),
			mcp.WithString("user", mcp.Description("登录用户名，默认 root")),
			mcp.WithString("group", mcp.Description("设备分组")),
			mcp.WithNumber("wait", mcp.Description("超时秒数，默认 30")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			srv := req.GetString("server", "")
			pwd := req.GetString("password", "")
			devid := req.GetString("device_id", "")
			command := req.GetString("command", "")
			user := req.GetString("user", "root")
			group := req.GetString("group", "")
			wait := int(req.GetFloat("wait", 30))

			result, err := execCommand(srv, pwd, devid, group, command, user, wait)
			if err != nil && result == nil {
				return mcp.NewToolResultError(err.Error()), nil
			}

			output := map[string]any{
				"exit_code": result.Code,
				"stdout":    result.Stdout,
				"stderr":    result.Stderr,
			}
			if err != nil {
				output["error"] = err.Error()
			}

			data, _ := json.MarshalIndent(output, "", "  ")
			return mcp.NewToolResultText(string(data)), nil
		},
	)

	// upload_file
	s.AddTool(
		mcp.NewTool("upload_file",
			mcp.WithDescription("上传本地文件到远程设备"),
			mcp.WithString("server", mcp.Description("rttys 服务器地址"), mcp.Required()),
			mcp.WithString("password", mcp.Description("rttys 登录密码"), mcp.Required()),
			mcp.WithString("device_id", mcp.Description("设备 ID"), mcp.Required()),
			mcp.WithString("file_path", mcp.Description("本地文件路径"), mcp.Required()),
			mcp.WithString("group", mcp.Description("设备分组")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			srv := req.GetString("server", "")
			pwd := req.GetString("password", "")
			devid := req.GetString("device_id", "")
			filePath := req.GetString("file_path", "")
			group := req.GetString("group", "")

			sent, err := uploadFile(srv, pwd, devid, group, filePath)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("upload failed (sent %d bytes): %v", sent, err)), nil
			}

			return mcp.NewToolResultText(fmt.Sprintf("uploaded %s (%d bytes)", filePath, sent)), nil
		},
	)

	// download_file
	s.AddTool(
		mcp.NewTool("download_file",
			mcp.WithDescription("从远程设备下载文件到本地"),
			mcp.WithString("server", mcp.Description("rttys 服务器地址"), mcp.Required()),
			mcp.WithString("password", mcp.Description("rttys 登录密码"), mcp.Required()),
			mcp.WithString("device_id", mcp.Description("设备 ID"), mcp.Required()),
			mcp.WithString("remote_path", mcp.Description("设备上的文件路径"), mcp.Required()),
			mcp.WithString("output_path", mcp.Description("本地保存路径，默认保存到当前目录")),
			mcp.WithString("group", mcp.Description("设备分组")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			srv := req.GetString("server", "")
			pwd := req.GetString("password", "")
			devid := req.GetString("device_id", "")
			remotePath := req.GetString("remote_path", "")
			outputPath := req.GetString("output_path", "")
			group := req.GetString("group", "")

			filename, received, err := downloadFile(srv, pwd, devid, group, remotePath, outputPath)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("download failed: %v", err)), nil
			}

			return mcp.NewToolResultText(fmt.Sprintf("downloaded %s (%d bytes)", filename, received)), nil
		},
	)

	return server.ServeStdio(s)
}
