Issues with the Optimizer
===

# Terminology

* _Heap Table_ = A base table (as opposed to an index)
* _Conglomerate_ = A location where data is stored (either a heap, or an index)
* _Optimizable_ = Abstract syntax representation of a table and all of its associated conglomerates (heap + all indices)
* _Predicate_ = A restriction on the output of a query (e.g. Foo = 'bar', a =3, b > 2, c is not null, etc)
* _Sargable Predicate_ = a Predicate which can be applied to the underlying conglomerate directly (without using a ProjectRestrict node)
* _Qualifier_ = same thing as a Sargable Predicate
* _Join Predicate_ = A predicate between 2 or more optimizables
* _Subquery Predicate_ = A predicate which involves a subquery (either correllated or uncorrelated)
* _Join Order_ = The sequence of tables to be joined
* _Access Path_ = The conglomerate *and join strategy* used to access data for a particular Optimizable


# Derby Optimization Algorithm
* Sequential costing algorithm
* Left-deep Join tree only
* Two phases to optimize: Join order and Access Path selection
* Overall performance is *O(N!\*sum(Ni))*, where *N* is the number of optimizables in the join, and *Ni* is the number of conglomerates for optimizable *i*
* Overall algorithm:
	1. Select an optimizable to be first in the join order
	2. Push any applicable predicates down to that optimizable (including Join predicates which may apply, but *not* including any Subquery predicates)
	3. Select an optimal access path based on predicates
	4. Evaluate the cost to access this table. If the cost exceeds any previously found "best" cost, then the access path is discarded
	4. Select an optimizable to be second in the join order
	5. Repeat steps 2 and 3 (using the "Depth-first search" described below) 
	6. Repeat steps 4-5 until all tables have been placed in the join order
	7. Pull all Predicates back off all optimizables
	8. Repeat steps 1-6 with a new join order

## Join Order algorithm
 
* Exhaustive search: Sees (eventually) all possible join orders for *N* optimizables
* scales as *O(N!)*, where *N* is the number of optimizables in the join sequence
* For larger than 6 tables, has optional timeout parameter to prevent runaway compile times. Not currently used by Splice

## Access Path Selection

* "Depth-first Search", as follows:
	1. Choose optimal conglomerate for accessing first optimizable in join order
	2. Evaluate the cost to pull data from this conglomerate. If the cost exceeds any previously found "best" cost, then the entire join order is discarded as too expensive.
	3. Repeat steps 1-2 for eaach optimizable in the join order until 

		A. The cost exceeds any previously found best cost

		B. The entire join order is determined
	
* Scales as *O(sum(Ni))*, since each Conglomerate is visited once and only once per join order selection

#Derby Cost Model

* Uses laboratory-obtained latency measurements as base cost score 
* Uses B-Tree measurements for row counts, page counts, etc.
* Uses collected cardinality measurements for estimating selectivities
* Favors indices over heaps *slightly* (for "concurrency reasons", which most likely has to do with Derby's transaction model)
* For non-qualifier predicates, chooses parameter-based constant selectivity
* Does not account for parallelism in query execution

# Costing within the Optimizer logic
* Each join order gets a new cost estimate
* Each Optimizable in the order adds a scan cost to the estimate
* When an Optimizable changes position in the join order, or changes access path, the old value *for that optimizable only* is subtracted out of the base cost.

# Fuji Cost Model
* Uses empirically-determined "cost" factors as base cost score. 
* Uses HBase-provided Store file size to provide rough estimate of row count
* Uses empirical parameters to represent selectivity and join selectivity estimates
* Does not account for parallelism in query execution

# Fuji Optimizer Model
* The Fuji optimizer model is essentially unchanged from Derby's, except for some small code-level changed to allow for parameterized costing

# Known issues with Fuji/Derby Optimizer

* DB-2878: The "Depth-first" Access Path Selection algorithm does not consider conglomerate combinations that are better scoring
* DB-2001: Specifying join strategies in subqueries resulted in an infinite loop when that strategy is not feasible
* DB-2450: The Optimizer takes an extremely long time to generate a plan when there are a large number of tables present
* DB-3155: IN-clauses result in poor plan choices because they are not costed appropriately
* DB-3133: Derby does not include Row Limits when costing plans
* DB-2974: TPCH-300g requires numerous hints in order to complete (e.g. poor plans are chosen)
* DB-2886: GroupBy clauses are not properly estimating output sizes, biasing the optimizer towards join strategies
* DB-2902: TPCH-100g chooses poor plans
* Merkle, RocketFuel, and basically all POCs require extensive hinting in order to choose proper plan
* MergeJoin was only applicable when over two conglomerate scans, limiting its applicability and hindering plan choice and performance for Harte hanks and others
* The above is a sample, not an exhaustive list, of all the problems known with Fuji/Derby. Some are already fixed, some were corrected with the introduction of a better cost model, and some are still outstanding

# Lassen Cost Model
* Uses cluster-measured latency as base cost score
* Makes distinction between "processing cost" and "remote cost"
	* _Processing Cost_ = the cost to read the data locally and perform local operations (addition, grouping internally, etc.)
	* _Remote Cost_ = the cost to scan the output rows over the network once all local operations are completed
* Takes parallelism into account by keeping track of input and output partitions, and by accurately representing execution algorithms (such as the cost to perform an index lookup)
* Applies costing separately for each element in a constant IN-clause
* Collects row counts, cardinalities, frequent elements, and row/column sizes to use for estimating the processing and remote costs
* When no statistics are present, uses HBase-provided region load information to estimate row counts, and parameters to guess at other factors (forthcoming)
* Uses "partitions" rather than "regions" as its unit of distribution. 
	* Allows a drop-in Spark costing as spark algorithms are introduced

# Lassen Optimizer Model

* There were no _planned_ changes to the optimizer except as necessary to correct for statistics-related issues.
* This plan is proving problematic, as the issues with the Derby optimizer are expanding

# Known Lassen issues

* DB-2887. TPCH1g is choosing poor plans
* Derby assumed that join strategies do not affect scan order, but MergeSortJoin does. 
	* This results in Merge join sometimes not being chosen despite being a better plan
	* Can also result in incorrectly eliding a sort when that is not possible
	* May have other (unforseen) effects
* Derby does not provide adequate information (particularly cardinalities) for all columns involved in a join predicate
	* Join selectivities are therefore poor (missing information for the outer portion of the join)
* Transitional bugs related to the changed cost model
* Without statistics, TPCH1g and Harte Hanks (and likely everyone) will choose poor plans
	* Partly due to above issues
	* In the Stats model, predicate and join selectivities are estimated based on measured statistics (cardinality in particular). Without measured statistics (row counts are not enough!), selectivity estimates become very poor

# Potential Optimizer Changes

## Join Order Algorithm Options
### Intended to solve
* DB-2878: More effective search space
* DB-2450: Search Space is too large, resulting in excessive optimization time.

### Options
* Exhaustive Search
	* Enumerate all possible join orders and access paths and choose optimal plan from this
	* Note that, with 4 join strategies (NestedLoop, Broadcast,MergeSort,and Merge), there are 4\**Ni* possible access paths for each optimizable, where *Ni* is the number of conglomerates for Optimizable *i*. This means that there are a total of *4\*sum(Ni)\*N!* possible plans. 
	* With 5 tables, each having 2 indices, this will examine *4\*10\*5!=4800* plans.
	* With 6 tables, each having 2 indices, will examine 28,800
	* With 7 tables, each having 2 indices, will examine 201,600
	* With 8 tables, each having 2 indices, will examine 1,612,800
* Genetic Algorithms (Postgres, 
* Dynamic Programming (Cascades, Orca)
* Greedy Algorithms (System R)
* Parallel Execution of any choice
	* Will require significant re-writing of AST logic in order to prevent race conditions and bugs

## Join tree options
* Bushy Trees (DB-2982)

## Costing changes
* Automatic statistics collection
* Histograms
* More accurate Frequent Elements algorithms
* Dynamic system measurements (latency, etc) to replace static collection. DB-3376

### Appendix: Cardinality and Predicate Selectivity issues
* Vanilla derby used statistics to acquire cardinality estimates.
* Fuji did not have cardinality estimates, so it used wholly parameter-based selectivity estimates
	* As a result, Fuji did not have accurate predicate selectivity
	* DB-2833, DB-2013, DB-2157 are all symptoms of poor predicate selectivity estimates
* Poor selectivity estimations don't matter when
	* We know that we have a primary key or unique index lookup. In this case, the output selectivity is always 1 row, so it doesn't matter what our estimate is
	* The table is so small massively over or under-estimating the selectivity will make no difference to the query plan. This is the case with most ITs
	* The system parameters have been explicitly tuned to have accurate estimates for this particular query and/or set of queries. This is the case with TPCH1g in Fuji
* In Lassen, statistics are available. This can make predicate selectivity estimates better(in particular, better for qualifiers which are not applied against a keyed column). However, making using of statistics requires that the old Fuji parameter-based system be removed (since the models change). 
	* When no statistics are available, Lassen is unable to defer to old Fuji parameter logic. This means that queries which were tuned in Fuji to be accurate under parameters must be re-tuned under the Lassen cost model.

