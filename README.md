# Elections in Bremen
Jan Lorenz

This repository provides data and data analyses on elections in Bremen,

- partly in English (for scientific analysis)
- teilweise in Deutsch (für die Bremische Öffentlichkeit)

## Voting System - short version

This explains how the distribution of Mandates to parties and candidates
are computed from the voting result for the parliament **Bremische
Bürgerschaft**.

### Input data

A dataframes including the vote counts for

- all lists (**list votes** with ID (=Kennnummer) inding with “00”) and
- all candidates (**person votes** with IDs ending with numbers from
  “01” to maximally “72”).

(The leading one or two digits in the ID identify the parties. Warning:
IDs for parties are not constant over time. The party with the most
votes gets “100” in the next elections and so by number of votes.
Therefore, for example, GRÜNE are “200” in 2015, because they were
second in 2011. Typically they are 300. CDU was “100” in 2023 because
they were first in 2019, in 2015 they were “300” and typically “200”.)

### Basic information

The following procedure has to processed for both voting districts
**Bremen** and **Bremerhaven** independently. For each district there
are a certain number of seats to be distributed first to parties and
then to their candidates. (Note, every candidate must be assigned to a
party list, there is no way to run independently of a list!)

The total number of seats per voting district can change for each
election. It is changed by decision of the Bürgerschaft on the basis of
proportionality to the populations in Bremen in Bremerhaven. These are
the numbers:

| Jahr | Wahlbezirk  | Sitze |
|------|-------------|-------|
| 2011 | Bremen      | 68    |
| 2011 | Bremerhaven | 15    |
| 2015 | Bremen      | 68    |
| 2015 | Bremerhaven | 15    |
| 2019 | Bremen      | 69    |
| 2019 | Bremerhaven | 15    |
| 2023 | Bremen      | 72    |
| 2023 | Bremerhaven | 15    |

The candidates elected in each district together form the Bremische
Bürgerschaft.

### Procedure mandate distribution for one district

1.  **Count all party votes:** For each party count all votes (list
    votes plus person votes for all their candidates).
2.  **5% threshold:** Calculate the fraction of the total number of
    votes each party has. Remove all parties which have less than 5% of
    the votes. Also remove their candidates. Only those parties
    remaining will get seats in the district.
3.  **Round to seats for parties:** *Round* the votes counts of the
    remaining parties down to integers representing the number of seats
    for that parties. The sum of the seats must equal the number in the
    table above. The rounding method used for this is the [Saint-Laguë
    method](https://en.wikipedia.org/wiki/Sainte-Lagu%C3%AB_method). A
    procedure it provided there. It is equivalent to a *divisor method*
    where you divide by a selected *divisor* (first choice is the total
    number of votes), then multiply by the total number of seats and
    round to the nearest integer (standard rounding), check if there is
    a mismatch in the total number of seats. If yes, adjust the divisor
    upwards to bring the total number of seats down or downwards to
    bring the total number of seats up. Repeat until you find a fitting
    divisor. There is only one matching seat distribution which can be
    found by this method, though several divisors can work.
4.  **Divide the seats for each party in two chunks, one for *list
    seats* and one for *person seats*:** Now you already have the seats
    for each party. These should be divided into two shares proportional
    to the number of list votes and candidate votes the party has. To
    that end, you must count all person votes for all candidates of the
    party. Now, you have two counts: The number of list votes and the
    numbr of person votes. Use the [Saint-Laguë
    method](https://en.wikipedia.org/wiki/Sainte-Lagu%C3%AB_method) to
    round these counts down to a *list vote share* and *person vote
    share* suming up to the total number of seats for that party.
5.  *For each party, assign candidates to the seats in the person vote
    share:* For each party list, rank the candidates by the number of
    votes and assign the seats of the person vote share to the
    candidates from the top of that list.
6.  *For each party, assign candidates to the seats in the list vote
    share:* For each party list, remove those candidates from the list
    who already have a seat from the person vote share. Now assign the
    seats of the list vote share by the original list of the party,
    lowest IDs/Kennnummern first.
