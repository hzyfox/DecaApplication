sbt compile
sbt package
cp target/scala-2.10/decaapplication_2.10-1.0.jar .
java  -XX:+PrintGCDetails -cp /Users/husterfox/.sbt/boot/scala-2.10.4/lib/scala-library.jar:decaapplication_2.10-1.0.jar microBenchmark.PRHelper 7 pr_70000 25 5 5 1 2 0
