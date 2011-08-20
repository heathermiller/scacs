name := "Scala Cluster Service"

version := "0.1"

scalaVersion := "2.9.0"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases" 

libraryDependencies ++= Seq(
                    "se.scalablesolutions.akka" % "akka-actor" % "1.2-RC3",
                    "se.scalablesolutions.akka" % "akka-remote" % "1.2-RC3"
)

