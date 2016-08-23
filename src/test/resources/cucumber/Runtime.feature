@all
Feature: Dynamic Key Grouping

  @countryDataSet
  Scenario Outline: Run tests with Country data set
    Given Data type and process duration
      | dataSet | processDuration | terminationDuration |
      | COUNTRY | 10              | 300000              |
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

      | SHUFFLE       | SKEW        | 1           | 10           |
      | KEY           | SKEW        | 1           | 10           |
      | PARTIAL_KEY   | SKEW        | 1           | 10           |
      | DYNAMIC_KEY   | SKEW        | 1           | 10           |

      | SHUFFLE       | SKEW        | 1           | 20           |
      | KEY           | SKEW        | 1           | 20           |
      | PARTIAL_KEY   | SKEW        | 1           | 20           |
      | DYNAMIC_KEY   | SKEW        | 1           | 20           |

      | SHUFFLE       | SKEW        | 1           | 50           |
      | KEY           | SKEW        | 1           | 50           |
      | PARTIAL_KEY   | SKEW        | 1           | 50           |
      | DYNAMIC_KEY   | SKEW        | 1           | 50           |

      | SHUFFLE       | SKEW        | 1           | 100          |
      | KEY           | SKEW        | 1           | 100          |
      | PARTIAL_KEY   | SKEW        | 1           | 100          |
      | DYNAMIC_KEY   | SKEW        | 1           | 100          |


#  @realDataSet
#  Scenario Outline: Run tests with Real data set


#  @elapsedTime
#  Scenario Outline: Algorithm elapsed time test
