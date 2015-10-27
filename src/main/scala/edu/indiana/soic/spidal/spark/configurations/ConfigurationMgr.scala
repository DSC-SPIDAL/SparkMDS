package edu.indiana.soic.spidal.spark.configurations

/**
 * Created by pulasthiiu on 10/26/15.
 */
import edu.indiana.soic.spidal.spark.configurations.section._;

object ConfigurationMgr{
  def LoadConfiguration(configurationFilePath: String): ConfigurationMgr = {
    return new ConfigurationMgr(configurationFilePath)
  }
}
class ConfigurationMgr (filePath: String) {
  var configurationFilePath: String = filePath;
  var damdsSection: DAMDSSection = new DAMDSSection(configurationFilePath)
}
