# The Problem

###Scenario 1: Stream of small inserts

User Alice has a table T. She continuously insert a small number of records into T, so that every N seconds we have M new mutations to the table data. Simultaneously, Alice wishes to perform some application-level queries of that data. Eventually, any stats which existed on that table become stale, with potentially negative consequences to any query choices which are made. 

###Scenario 2: Large import/insert
Alice wishes to insert a large number of records into a table. This immediately makes all statistics stale (if any had previously been collected), and previous query choices are likely to be poor.

###Scenario 3: Temp Tables
Bob creates a Temporary table, then inserts a very large number of records into the table. Since the Temporary table has no statistics, any queries made against that table may be poorly planned.

#Success Criteria

1. Without needing to explicitly collect stats, we choose plans which perform as well in LASSEN as they did in FUJI for:
  1. TPCH
  2. Harte Hanks
2. When statistics are collected, we perform optimally (barring unrelated bugs)
3. There is an effective story for query planning for each of the three above scenarios


# Solutions

## Do Nothing

### Overview
We do nothing over current master

### Pros
1. very little work

### Cons
1. In Scenario 1, Alice must compute how long she can go with the same plan being chosen, then make sure she manually recomputes statistics within that window
2. In Scenario 2, Alice must compute statistics immediately upon completing the import. If she does not, plans chosen are uniformly poor.
3. In Scenario 3, Bob must compute statistics on his temporary table immediately upon completing any insertion which occurs, and before performing any reads against that. This usually imposes some latency costs.
4. TPCH1g will not complete without manually collecting statistics, because the plan choices are very poor.
5. Harte Hanks will not complete without manually collecting statistics, because the default plan choices are too poor.

### Notes
If this were a real option, we would not need this document. It is here to provide a baseline by which we can compare the other approaches. It mainly outlines the main problems that we expect to see in the absence of action. 

## Parameterized costing when statistics are absent

This is essentially the FUJI approach, but modified to fit within the newer optimizer scoping and to reflect bugs fixed in Lassen for the optimizer.

### Overview

1. Use stale statistics over no statistics at all. 
2. When no statistics at all exist:
  1. Use HBase region load information to acquire region sizes. 
	2. Sample queries to measure local and remote latency costs for each table.
  2. Use the acquired region sizes to estimate row counts for each partition
	3. Use pre-configured parameters to provide cardinality and selectivity estimates (e.g. = clause has a 0.9 selectivity, and each row has 200 duplicates, etc.)

###Pros
1. Some code is already written (the Region load information)
2. Slightly less invasive, because we write a backing Statistics Store to turn region load information into actionable stats
3. In Scenario 3, we would be slightly better than the baseline, because we would use some measured information about the size of the data set

###Cons
1. Aside from the total byte size (which may even be a bit more accurate than collected stats), all statisitics are significantly less accurate (They aren't being measured, just guessed at arbitrarily). This decreased accuracy means that it can never be as good at costing as having statistics will be. As a result, we will constantly be using hints any time we don't want to call COLLECT\_STATS
2. We can game the system to make our tests work better, but that tells us nothing about how it will work in a non-test environment (particularly with respect to Scenario 3)
3. In Scenario 1, Alice's query choice would be exceptionally poor, because the query plan will change only with size, and will not reflect any changes in cardinality or distribution which may take effect. This will translate into favoring indices over base tables even when the base table is superior, because the index size will be smaller relative to the base table. In reality, the IO savings of the Index will be outweighed by the network cost of performing index lookups, but we have no way of knowing that. 
4. In Scenario 2, Alice's plans will become stale, but some plans will become more expensive, because we will assume that the distribution of keys is incorrect, and thus we would choose incorrect indices.
5. Unusual when compared to competitors: No competing database (Oracle, Postgres, SQL Server, MySQL etc) uses this approach: In all cases, stats collection of one form or another is required for good plans to be chosen: This scares me, since it's such an easy design to come up with. This tells me that our competitors would rather force the user to collect statistics than give them a poor second-cousin, and I find it wise to follow their lead.
6. While the constructive part of the solution(i.e. the adding of new code paths) won't be as invasive, dealing with the consequences in the optimizer will likely be significant, in light of several large changes made for Harte Hanks et al (i.e. DB-3155, DB-2887, and others).

###Steps
1. Integrate in Region Load information into statistics. This is done in a local branch
2. Adjust parameters until optimal plans are chosen for some set of queries
3. Correct or re-write any sections of the optimizer which are implicitly relying on accurate statistics, so that they work well in either case. E.g. we would need to deal with cases when statistics are not "real" everywhere we do costing, lest we generate an unacceptably poor plan.
4. Hope that we didn't miss any queries that break badly under this strategy

###Notes
1. The time required here is mainly going to be spent testing and tweaking parameters--e.g. try a query, see the plan, tweak some parameters until the plan comes out better, then re-test all prior queries to make sure the tweaking didn't break any of them. Then move to the next query. 
2. I expect that at least some of the general changes that I've made to the optimizer (i.e. integrating MergeJoin in more effectively, dealing with IN clauses correctly, and so forth) will mean that even with this in place, the optimizer will still make worse choices than it did in FUJI, just because that's the way of the world. I'm particularly worried about DB-3155 and it's consequences, since it affects Harte Hanks considerably. 
3. It is hard to know if there is a single set of parameters which can be used for all regression test suites. That particular optimization problem may not have a globally optimal solution, and the more regression tests we add, the harder it will be to find.
4. I would expect 2 informed developers to spend 2 weeks on the testing and tweaking phase

###Estimated Development Time Required: 2-3 days
###Estimated Testing Time Required: 2 weeks
###Doc Impact:
1. We will need to make some kind of mention about how the query optimizer makes plans around temporary tables. 
2. Because we would be using a "fallback" strategy which is known to be inferior, we will need to strongly recommend statistics collections at every appropriate location.

## Auto-collect
The main idea is to automatically collect statistics, so that there's never a situation in which stats aren't available, or at least, that such a window is very small.

### Overview
1. Use stale statistics over no statistics at all.
2. All Orderable columns are considered enabled for collection unless explicitly disabled. This is a reverse of our current approach, but since it hasn't left the nest yet, a fairly minor change.
2. On startup for a region, check the last updated timestamp for that region's stats. If no stats exist, or the timestamp exceeds a configured "staleness threshold", then initiate a statistics collection
3. Put a write listener on each region. As writes are received, update the number of rows which are mutated. If the number of rows exceeds a configured threshold, then the region is considered stale, and a collection is initiated
4. Collections are maintenance tasks (like RollForwards) and operate in a rate-limited environment. Since they are read-only, the rate limiting only needs to occur on the read-side.

### Pros
1. This is Statistics. There is only one code path and one approach--all the optimizer ever needs to worry about is the statistics model
2. Already part of the plan. This was pushed to Post-lassen for time purposes, but was always considered a significant part of the statistics design. 
3. Can be implemented by re-using already tested components of the existing Statistics architecture. For example, there's no need to write a new Statistics collector algorithm, because one already exists. We merely have to write the mechanisms to trigger such collections.
4. In Scenario 1, statistics collections would automatically be triggered based on a single pre-configured threshold limit (number of rows modified), and thus Alice would not need to intervene
5. In Scenario 2, statistics collections would have been triggered already a few times by the time the import has completed, so there would never be a time when statistics are stale
6. In Scenario 3, becuase stats are already collected, there would be no need for Bob to explitely collect stats, and the automatic trigger would (hopefully) reduce his latency between populating the temporary table and performing the next steps.
7. Would have the fewest long-term consequences, since there is no fundamental model change.

### Cons
1. This is a feature written out of lassen that has to be brought back in.
2. QA would need to consider this as part of their testing plans 
3. Would take longer to construct initially than other options

### Steps
1. Write a staleness detector that can run on each region. This is pretty much the logic in SegmentedRollForward, so there's not a lot of thinking required, just implementing new triggering mechanisms
2. Write a mechanism that allows us to disable automatic stats collection. This is trivial, and would just disable the staleness detector
3. (Optional) Include the ability to manually collect all stale regions through the staleOnly flag placed on the Stored Procedure. As this was designed for, it's unlikely to prove much impediment
4. Test that rate-limited scans and collections do not significantly impede concurrent operations--e.g. that resources are well managed with automatic collection enabled.
5. Test race conditions around things like "finish the import and immediately call a complex query." to make sure that the choices made during that time are acceptable.

###Notes
1. Steps 1-3 are the constructive portions. 2 informed developers should be able to accomplish them in a week with no other distractions.

###Estimated Development Time Required: 1 week
###Estimated Testing Time Required: 2-3 days internal testing, and then whatever time QA would need to test a new feature
###Doc Impact:
1. We will need to add a section to the docs about Automatic collection, and what triggers it.
2. We will need to change the information about the default-on policy for column statistics
3. We will need to change the information about the staleOnly flag in the Stored Procedures to reflect the auto-collect portions
