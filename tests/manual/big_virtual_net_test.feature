Feature: Big network test
    Scenario: Normal work
        When start virtual network with 100 nodes
        Then I collect topology from every node

        When stop 50 nodes
        And wait 80 seconds
        Then I collect topology from every node

