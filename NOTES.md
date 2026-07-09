Jul 9, 26
The design currently conflates three different concepts:
1. a logical event such as one sensor reading
2. a signal update such as the newest robot pose
3. an engine activation such as a timer or a wake notification

Bc the pending work is a bitset, a node runs at most once per engine step. That is valuable for a diamond configuration, but it also means several independent events can be reduced to one node invocation.

This has some serious implications:
1. `merge` keeps only the first input when several inputs fire in one step
2. `delay` can overwrite earlier due values while draining several at once
3. `collapse` keeps only the last item in a burst
4. recording repro the burst partition observed during one of the live runs, but the original
business result can still depend on OS wake timing
5. each live payload also enqueues a separate wake, even though duplicate wakes for one node collapse to one pending bit
6. generic live input is commonly unbounded, while bounded worker sends may block the only graph thread

I think the core direction for morel next is:

Keep one topological, glitch free cascade per logical event. Permit multiple cascades at the 
exact same `Time`, in deterministic order. Perhaps treat batching and latest-value coalescing as explicit user choices (not sure if this adding undue complexity, but this seems important?) and never accidental products of scheduler timing.

I think the current problem can be summarized as "deterministic replay of non-deterministic batching". 

Thinking through the problem that we are attempting to solve: multiple events with the same timestamp. Morel currently treats same timestamp as one combined engine step and that is the mistake.

There are 4 different situations here:
1. Multiple independent root events with the same time.
2. Multiple outputs produced from the same root event.
3. Atomic groups of values that must be observed together.
4. Out of order event time, watermarks, and late-data handling.

First one is straightforward, 

pop one (time, sequence, root)
run one cascade
pop the next

The engine must stop draining all equal time timers into one pending bitset. It pops one activation and completes it before popping another.

Second is much more complicated. A source event fans into two branches, and both branches entermerge for e.g. Now one root has caused two legit merge outputs. Solving this is quite complicated. Do we add buffering, defined ordering policy? Not sure. 

Third, sometimes two values must be observed together as a single transaction or sensor frame. That should probably be explicit like stream<batch<T>> or combine_fired. 

Out of order event time is a much larger problem and should likely be deferred. 

Likely requirement is probably something like: same timestamp does not mean same event, same batch, or simultaneous execution. It only means the engine block doesn't advance between two sequential events. 
