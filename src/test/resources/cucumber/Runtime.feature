@all
Feature: Run all tests

  Scenario Outline: Run tests with Country data set
    Given Data type and process duration
      | dataType | processDuration | terminationDuration |
      | COUNTRY  | 10              | 10000               |
    When Grouping type is <GROUPING TYPE>
    And Stream type is <STREAM TYPE>
    And Spout count is <SPOUT COUNT>
    And Worker count is <WORKER COUNT>
    Then Execute test
      | retryCount |
      | 5          |
    And Test successfully completed

    Examples:
      | GROUPING TYPE | STREAM TYPE | SPOUT COUNT | WORKER COUNT |
      | SHUFFLE       | SKEW        | 1           | 5            |
      | KEY           | SKEW        | 1           | 5            |
      | PARTIAL_KEY   | SKEW        | 1           | 5            |
      | DYNAMIC_KEY   | SKEW        | 1           | 5            |
