lazy val sparkWorkshop = project.in(file("."))
  .settings(
    name := "SparkWorkshopV2",
    version := "0.1",
    scalaVersion := "2.11.12",
    libraryDependencies ++= {
      val sparkV = "2.3.2"
      val slickV = "3.2.3"
      val pgDriverV = "42.2.5"
      val sparkDeps = Seq(
        "spark-core",
        "spark-sql",
        "spark-streaming"
      ).map("org.apache.spark" %% _ % sparkV)

      val slickDeps = Seq(
        "slick",
        "slick-hikaricp"
      ).map("com.typesafe.slick" %% _ % slickV)

      sparkDeps ++ slickDeps ++ Seq(
        "org.postgresql" % "postgresql" % pgDriverV
      )
    }
  )