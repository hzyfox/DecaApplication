name := "DecaApplication"

version := "1.0"

scalaVersion := "2.10.3"
resolvers += Resolver.url("bintray-sbt-plugins", url("http://dl.bintray.com/sbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.1"
javacOptions ++= Seq("-source", "1.6", "-target", "1.6")

libraryDependencies ++= Seq(
  "org.scalanlp" %% "breeze" % "0.11.2",
  "org.scalanlp" %% "breeze-natives" % "0.11.2",
  "org.scalanlp" %% "breeze-viz" % "0.11.2"
)
 assemblyMergeStrategy in assembly := {
   case x =>
     val oldStrategy = (assemblyMergeStrategy in assembly).value
     oldStrategy(x)
  }
resolvers ++= Seq(
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Release" at "https://oss.sonatype.org/content/repositories/releases/"
)
    
