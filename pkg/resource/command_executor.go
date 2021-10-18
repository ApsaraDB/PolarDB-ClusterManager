package resource

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mitchellh/go-homedir"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"golang.org/x/crypto/ssh"
	"io/ioutil"
	"os/exec"
	"time"
)

type CommandExecutor interface {
	Exec(cmd []string, retryTimes int, timeout time.Duration) (string, string, error)
}

type HostCommandExecutor struct {
	Endpoint common.EndPoint
}

type SshCommandExecutor struct {
	Host string
}

func publicKeyAuthFunc(kPath string) (ssh.AuthMethod, error) {
	keyPath, err := homedir.Expand(kPath)
	if err != nil {
		return nil, errors.Wrapf(err, "find key's home dir failed")
	}
	key, err := ioutil.ReadFile(keyPath)
	if err != nil {
		return nil, errors.Wrapf(err, "ssh key file read failed")
	}
	// Create the Signer for this private key.
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, errors.Wrapf(err, "ssh key signer failed")
	}
	return ssh.PublicKeys(signer), nil
}

func (e *SshCommandExecutor) Exec(cmd []string, retryTimes int, timeout time.Duration) (string, string, error) {
	key := "~/.ssh/id_rsa"
	user := *common.WorkUser
	// get auth method
	method, err := publicKeyAuthFunc(key)
	if err != nil {
		return "", "", err
	}
	auth := []ssh.AuthMethod{method}

	clientConfig := &ssh.ClientConfig{
		User:            user,
		Auth:            auth,
		Timeout:         timeout,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	// connet to ssh
	addr := fmt.Sprintf("%s:%d", e.Host, 22)

	sshClient, err := ssh.Dial("tcp", addr, clientConfig)
	if err != nil {
		return "", "", err
	}
	defer sshClient.Close()

	// create session
	session, err := sshClient.NewSession()
	if err != nil {
		return "", "", err
	}
	defer session.Close()

	command := ""
	for _, c := range cmd {
		command += c + " "
	}
	log.Infof("exec %s on host %s", command, e.Host)

	out, err := session.CombinedOutput(command)
	if err != nil {
		return string(out), "", err
	}

	return string(out), "", err
}

func (e *HostCommandExecutor) Exec(cmd []string, retryTimes int, timeout time.Duration) (string, string, error) {
	url := "/v1/exec"
	param := map[string]interface{}{
		"cmd":     cmd,
		"timeout": timeout.Seconds(),
		"retry":   retryTimes,
	}

	client := &EndpointHttpClient{
		Endpoint:   []common.EndPoint{e.Endpoint},
		RetryTimes: retryTimes,
		Timeout:    int(timeout.Seconds() + 1),
	}
	err, res := client.PostRequest(url, param, false)
	if err != nil {
		log.Warnf("Failed to request %s err:%v res:%s", url, err, res)
		return "", "", err
	}

	var apiResp common.ApiResp
	err = json.Unmarshal([]byte(res), &apiResp)
	if err != nil {
		log.Warnf("Failed to Unmarshal res %s err %s", res, err.Error())
		return res, "", err
	}

	if apiResp.Code != 200 {
		return apiResp.Msg, "", errors.New(res)
	}

	return apiResp.Msg, "", nil
}

func (e *HostCommandExecutor) LocalExec(cmd []string, retryTimes int, timeout time.Duration) (string, string, error) {
	if len(cmd) == 0 {
		return "", "", errors.New("empty cmd")
	}
	arg := cmd[1:]
	var cancel context.CancelFunc
	var ctx context.Context
	if timeout != 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()

	for {
		executor := exec.CommandContext(ctx, cmd[0], arg...)
		out, err := executor.Output()
		if err != nil {
			log.Warnf("Exec %v return %s, %s retry %d", cmd, out, err.Error(), retryTimes)
			if retryTimes <= 0 {
				return string(out), "", err
			}
			retryTimes--
		} else {
			return string(out), "", nil
		}
	}
}

type K8sCommandExecutor struct {
	Namespace     string
	PodName       string
	ContainerName string
}

func (e *K8sCommandExecutor) Exec(cmd []string, retryTimes int, timeout time.Duration) (string, string, error) {
	return ExecInPodWithRetry(cmd, e.ContainerName, e.PodName, e.Namespace, retryTimes, timeout)
}
