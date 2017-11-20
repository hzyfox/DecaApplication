name := "DecaApplication"

version := "1.0"


javacOptions ++= Seq("-source", "1.6", "-target", "1.6")

resolvers ++= Seq(
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Release" at "https://oss.sonatype.org/content/repositories/releases/"
)
    