select family, isactive, count(*) as scenarios
from public.motionmodelscenario
group by family, isactive
order by family, isactive desc;

select
    count(*) as total_scenarios,
    count(distinct signalrule) as distinct_signalrules,
    min(createdat) as first_createdat,
    max(createdat) as last_createdat
from public.motionmodelscenario;

select
    s.family,
    r.passedconstraints,
    count(*) as results,
    avg(r.usefulpct) as avg_usefulpct,
    avg(r.stoppct) as avg_stoppct,
    avg(r.avgsecondstoriskfree) as avg_secondstoriskfree,
    avg(r.avgmaxadverse) as avg_maxadverse
from public.motionmodelresult r
join public.motionmodelscenario s on s.id = r.scenarioid
group by s.family, r.passedconstraints
order by s.family, r.passedconstraints desc;

select
    s.scenarioname,
    s.family,
    r.signalrule,
    r.fromts,
    r.tots,
    r.signals,
    r.targets,
    r.riskfree,
    r.stops,
    r.nodecision,
    r.targetpct,
    r.usefulpct,
    r.stoppct,
    r.avgsecondstoriskfree,
    r.avgmaxadverse,
    r.avgscore,
    r.profitproxy,
    r.passedconstraints,
    r.createdat
from public.motionmodelresult r
join public.motionmodelscenario s on s.id = r.scenarioid
order by
    r.passedconstraints desc,
    r.usefulpct desc nulls last,
    r.avgsecondstoriskfree asc nulls last,
    r.avgmaxadverse asc nulls last,
    r.signals desc nulls last,
    r.createdat desc
limit 50;
