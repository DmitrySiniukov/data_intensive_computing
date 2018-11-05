name := "CryptoSentiment"
version := "1.0"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1",
  "org.apache.spark" %% "spark-sql" % "2.2.1",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.1",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.0"
)

libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "5.0.2" excludeAll ExclusionRule(organization = "org.apache.spark")
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp"))
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

// For JSON parsing
resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
libraryDependencies +=  "com.typesafe.play" %% "play-json" % "2.3.0"