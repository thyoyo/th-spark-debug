# 本地spark调试

1. 下载spark对应版本包，添加jars到本地lib下，build.sbt指定lib（spark2.0需要scala 2.11以上版本，jdk8支持）
2. 利用spline显示更优美UI（需要jdk8和mongo支持）https://absaoss.github.io/spline/ 仅支持spark 2.2使用，需要使用sparksession