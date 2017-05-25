package org.apache.spark.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.jd.bdp.{OpenFalcon, OpenFalconReporter}
import org.apache.spark.SecurityManager
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem

/**
  * Created by tangshangwen on 17-3-6.
  */
class OpenFalconSink(val property: Properties,
                                    val registry: MetricRegistry,
                                    securityMgr: SecurityManager) extends Sink with Logging {

  logInfo("OpenFalconSink initialized")

  val OPEN_FALCON_DEFAULT_PERIOD = 60
  val OPEN_FALCON_DEFAULT_UNIT = "SECONDS"
  val OPEN_FALCON_DEFAULT_PREFIX = ""

  val OPEN_FALCON_KEY_HOST = "host"
  val OPEN_FALCON_KEY_PORT = "port"
  val OPEN_FALCON_KEY_PERIOD = "period"
  val OPEN_FALCON_KEY_UNIT = "unit"
  val OPEN_FALCON_KEY_PREFIX = "prefix"
  val OPEN_FALCON_KEY_TAGS = "tags"

  def propertyToOption(prop: String): Option[String] = Option(property.getProperty(prop))

  val host = propertyToOption(OPEN_FALCON_KEY_HOST).getOrElse("127.0.0.1")
  val port = propertyToOption(OPEN_FALCON_KEY_PORT).getOrElse("1988").toInt

  val pollPeriod = propertyToOption(OPEN_FALCON_KEY_PERIOD) match {
    case Some(s) => s.toInt
    case None => OPEN_FALCON_DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = propertyToOption(OPEN_FALCON_KEY_UNIT) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase)
    case None => TimeUnit.valueOf(OPEN_FALCON_DEFAULT_UNIT)
  }

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val prefix = propertyToOption(OPEN_FALCON_KEY_PREFIX).getOrElse(OPEN_FALCON_DEFAULT_PREFIX)

  private val tags = propertyToOption(OPEN_FALCON_KEY_TAGS).getOrElse("")

  // extract appId and executorId from first metric's name
  var appId: String = null
  var executorId: String = null

  if (!registry.getNames.isEmpty) {
    // search strings like app-20160526104713-0016.0.xxx or app-20160526104713-0016.driver.xxx
    val pattern = """(application-\d+-\d+)\.(\d+|driver)\..+""".r

    registry.getNames.first match {
      case pattern(app, executor) =>
        this.appId = app
        this.executorId = executor
      case _ =>
    }
  }

  val openFalcon = OpenFalcon.forService("http://" + host + ":" + port).create()

  lazy val reporter: OpenFalconReporter = OpenFalconReporter.forRegistry(registry)
    .prefixedWith(prefix)
    .removePreFix(Option(appId).map(_ + "." + executorId + ".").orNull)
    .withTags(tags)
    .withStep(pollPeriod)
    .build(openFalcon)

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }

}
