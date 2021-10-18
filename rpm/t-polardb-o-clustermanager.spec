Summary: PolarDB ClusterManager
#软件包的名字
Name: t-polardb-o-clustermanager
#软件包的主版本号
Version: 2.2.0
#软件包的次版本号
Release: %(echo $RELEASE)%{?dist}
#源代码包，默认将在上面提到的SOURCES目录中寻找
Source0: %{name}-%{version}.tar.gz
#授权协议
License: Commercial
#软件分类
Group: alibaba/applications
#软件包的内容介绍
%description
PolarDB ClusterManager服务
#表示预操作字段，后面的命令将在源码代码BUILD前执行

%define __os_install_post %{nil}

%prep
#BUILD字段，将通过直接调用源码目录中自动构建工具完成源码编译操作
%build
cd $OLDPWD/../rpm
sh build_rpm.sh

%install
# 二进制执行文件
cd $OLDPWD/../
mkdir -p ${RPM_BUILD_ROOT}/usr/local/polardb_cluster_manager/bin/
cp -f ./gopath/src/gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/polardb-cluster-manager ${RPM_BUILD_ROOT}/usr/local/polardb_cluster_manager/bin/polardb-cluster-manager
cp -f ./gopath/src/gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/tools/polardb_cluster_manager_control.py ${RPM_BUILD_ROOT}/usr/local/polardb_cluster_manager/bin/polardb_cluster_manager_control.py
cp -f ./gopath/src/gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/tools/supervisor.py ${RPM_BUILD_ROOT}/usr/local/polardb_cluster_manager/bin/supervisor.py

mkdir -p ${RPM_BUILD_ROOT}/usr/local/polardb_cluster_manager/bin/plugin/manager
cp -f ./gopath/src/gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/plugin/bin/manager/manager.so ${RPM_BUILD_ROOT}/usr/local/polardb_cluster_manager/bin/plugin/manager/manager.so
cp -f ./gopath/src/gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/plugin/conf/manager/manager_extern.conf ${RPM_BUILD_ROOT}/usr/local/polardb_cluster_manager/bin/plugin/manager/manager_extern.conf
cp -f ./gopath/src/gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/plugin/conf/plugin_golang_manager.conf ${RPM_BUILD_ROOT}/usr/local/polardb_cluster_manager/bin/plugin/plugin_golang_manager.conf

# 日志目录

# 配置文件
#mkdir -p ${RPM_BUILD_ROOT}/etc/refresh_agent
#cp -f /devops/app/go/src/refresh_agent/etc/online.config.ini ${RPM_BUILD_ROOT}/etc/refresh_agent/config.ini

#logrotate
#mkdir -p ${RPM_BUILD_ROOT}/etc/logrotate.d
#cp -f /devops/app/go/src/refresh_agent/etc/refresh_agent_logrotate.conf ${RPM_BUILD_ROOT}/etc/refresh_agent/refresh_agent_logrotate.conf

# 控制脚本
#mkdir -p ${RPM_BUILD_ROOT}/etc/init.d
#cp -f /devops/app/go/src/refresh_agent/scripts/refresh_agent.sh  ${RPM_BUILD_ROOT}/etc/init.d/refresh_agent


%post
# 添加开机自启动
# 更改权限
#chmod 775 /etc/init.d/refresh_agent
# 加入自启动
#chkconfig --add refresh_agent

#echo -e "has added refresh_agent to chkconfig \n"


# 安装启动
#/etc/init.d/refresh_agent start
#echo -e "start log rewrite to /var/log/messsge \n"


#调用源码中安装执行脚本
#文件说明字段，声明多余或者缺少都将可能出错
%files
%defattr(-,root,root)
/usr/local/polardb_cluster_manager/bin/polardb-cluster-manager
/usr/local/polardb_cluster_manager/bin/supervisor.py
/usr/local/polardb_cluster_manager/bin/polardb_cluster_manager_control.py
/usr/local/polardb_cluster_manager/bin/plugin/manager/manager.so
/usr/local/polardb_cluster_manager/bin/plugin/manager/manager_extern.conf
/usr/local/polardb_cluster_manager/bin/plugin/plugin_golang_manager.conf

%dir
#/etc/refresh_agent
#/bbd/logs/refresh_agent
