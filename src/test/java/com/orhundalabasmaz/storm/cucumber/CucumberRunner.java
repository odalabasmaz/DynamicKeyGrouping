package com.orhundalabasmaz.storm.cucumber;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

/**
 * @author Orhun Dalabasmaz
 */
@CucumberOptions(features = "src/test/resources/cucumber/", tags = "@all")
@RunWith(Cucumber.class)
public class CucumberRunner {
}
