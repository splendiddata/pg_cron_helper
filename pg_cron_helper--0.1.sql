/*
 * Copyright (c) Splendid Data Product Development B.V. 2023
 *
 * This program is free software: You may redistribute and/or modify under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at Client's option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program.  If not, Client should obtain one via www.gnu.org/licenses/.
 */

 do $do$
 declare
     cron_db_name         name;
     result_str           text;
     cron_server_present  text;
     sql                  text;
 begin
     select setting into cron_db_name
     from pg_settings
     where name = 'cron.database_name';
     if not found then
         raise exception 'The pg_cron_helper extension needs the pg_cron extension to be installed in a database in the cluster';
     end if;
     
     /*
      * Foreign data wrapper stuff to reach the pg_cron server database and for dblink connections
      * to accommodate autonomous transactions.
      */
     execute format ('create server if not exists cron_ctrl_server
         foreign data wrapper postgres_fdw
         options(dbname %L)', cron_db_name);
     comment on server cron_ctrl_server is 'For communication with the Cron control database';
     execute format ('create user mapping if not exists for current_user
         server cron_ctrl_server
         options (user %L)', current_user);

    if current_database() = cron_db_name then
        /*
         * This part only goes into the pg_cron server database - the database in which pg_cron is installed
         */
        if not exists (select * from pg_extension where extname = 'pg_cron') then
            raise exception 'The pg_cron_helper extension needs the pg_cron extension to be installed in a database in the cluster';
        end if;

        /*
         *
         */
        create table job_definition
            ( pk                   bigint  generated always as identity primary key
            , database_name        name         not null
            , user_name            name         not null
            , job_name             varchar(128) not null 
            , job_action           text         not null
            , start_date           timestamp with time zone not null default clock_timestamp()
            , repeat_interval      text         not null default ''
            , end_date             timestamp with time zone
            , auto_drop            boolean      not null default true
            , comments             text         not null default ''
            , jobid                bigint
            , cron_pattern         text         not null default ''
            , constraint uq_job_definition unique(database_name, user_name, job_name)
            , constraint fk_job_definition_cron_job 
                  foreign key (jobid)
                  references cron.job(jobid)
                  on update cascade
                  on delete set null
            );
        perform pg_catalog.pg_extension_config_dump('job_definition', '');
        comment on table cron.job_definition is 'Contains job definitions';
        comment on column cron.job_definition.pk is 'Technical primary key';
        comment on column cron.job_definition.database_name is 'Defines in which database the job will be executed. Also part of the unique identification of a job.';
        comment on column cron.job_definition.user_name is 'Part of the uniwue identification of a job';
        comment on column cron.job_definition.job_name is 'Unique identification of the job witin user_name and database_name';
        comment on column cron.job_definition.job_action is 'Contains the sql script that is to be executed';
        comment on column cron.job_definition.start_date is 'Timestamp after which the job is to be scheduled if repeat_interval is not empty';
        comment on column cron.job_definition.repeat_interval is 'Cron pattern or Oracle style interval definition
If empty, the job is just defined, not scheduled. It can be activated using the cron.job_run() procedure';
        comment on column cron.job_definition.end_date is 'Timestamp after which the job will not be scheduled any more';
        comment on column cron.job_definition.auto_drop is 'Not effective yet';
        comment on column cron.job_definition.comments is 'Just comment on the job';
        comment on column cron.job_definition.jobid is 'Refers to the cron.job table if the job is scheduled';
        comment on column cron.job_definition.cron_pattern is 'The cron pattern that is currently used by this job
If it starts with ''='', then a single run is scheduled - the pattern must be recaclulated for the next run.
If it starts with ''>'', then the next run was more than a year in the future on calculation time. The job action will
                       not be executed - a new run timestamp will be calculated and scheduled
If empty, the job is disabled - not scheduled
Otherwise a cron pattern that will define the next execution(s)';

        /*
         * table job_run
         */
        create table job_run
            ( job_definition_pk      bigint       not null
            , job_run_details_runid  bigint       primary key
            , constraint fk_job_run_job_definition
                  foreign key (job_definition_pk)
                  references cron.job_definition (pk)
                  on delete cascade
            , constraint fk_job_run_job_run_details 
                  foreign key (job_run_details_runid)
                  references cron.job_run_details(runid)
                  on delete cascade
            );
        create index job_run_job_definition_pk on cron.job_run(job_definition_pk);
        comment on table cron.job_run is 'Connects the cron.job_run_details table to the cron.job_definition table.
Rationale: In time a job definition may cause several cron.job rows, so connecting the job definition to
job run detail via the job table may not always be a good idea.';
        comment on column cron.job_run.job_definition_pk is 'reference to the cron.job_definition table';
        comment on column cron.job_run.job_run_details_runid is 'reference to the cron.job_run_details table';

        /*
         * procedure _srvr_create_job
         */
        create procedure _srvr_create_job
            ( p_database_name        name
            , p_user_name            name
            , p_job_name             varchar(128)
            , p_job_action           text
            , p_start_date           timestamp with time zone
            , p_repeat_interval      varchar
            , p_end_date             timestamp with time zone
            , p_enabled              boolean
            , p_auto_drop            boolean
            , p_comments             text
            ) security definer language plpgsql as $body$
        declare
        begin
            insert into cron.job_definition 
                ( database_name, user_name, job_name, job_action, start_date, repeat_interval, end_date, auto_drop, comments )
            values
                ( p_database_name, p_user_name, p_job_name, p_job_action, p_start_date, p_repeat_interval, p_end_date, p_auto_drop, p_comments);
        
            if p_enabled then
                call cron._srvr_enable_job(p_database_name, p_user_name, p_job_name);
            end if;
        end -- _srvr_create_job
        $body$;
        comment on procedure cron._srvr_create_job is 'Internal function define a job in the cron server database';

        /*
         * procedure _srvr_enable_job
         */
        create procedure _srvr_enable_job
            ( p_database_name        name
            , p_user_name            name
            , p_job_name             varchar(128)
            ) security definer language plpgsql as $body$
        declare
            v_derinition_pk    bigint;
            v_jobid            bigint;
            v_cron_pattern     text;
            v_start_date       timestamp with time zone;
            v_repeat_interval  text;
            v_end_date         timestamp with time zone;
            v_action           text;
        begin
            select pk, jobid, start_date, repeat_interval, end_date, job_action
              into v_derinition_pk, v_jobid, v_start_date, v_repeat_interval, v_end_date, v_action 
            from cron.job_definition
            where database_name = p_database_name
            and user_name = p_user_name
            and job_name = p_job_name;
            if not found then
                raise exception 'job ''%'' is not known in database ''%''', p_job_name, p_database_name;
            end if;
            if v_jobid is not null then
                raise debug 'job ''%'' is database ''%'' is already enabled', p_job_name, p_database_name;
                return;
            end if;
            if v_repeat_interval = '' then
                raise exception 'job ''%'' needs a repeat interval to be enabled', p_job_name;
            end if;
            
            v_cron_pattern := cron._srvr_make_cron_patten(v_start_date, v_end_date, v_repeat_interval);
            if v_cron_pattern = '' then
            	 raise exception 'job ''%'' cannot be enabled as the next run will be after the end date %', p_job_name, v_end_date;
            elsif v_cron_pattern ~ '^>' then
                v_jobid =  cron.schedule_in_database( job_name => format('%s:%s', p_database_name, p_job_name)
                                                    , schedule => substr(v_cron_pattern, 2)
                                                    , command => format('call cron._cron_job_execution(%L, %L, false, ''select 1;'');', p_user_name, p_job_name)
                                                    , database => p_database_name
                                                    , username => p_user_name
                                                    );
            elsif v_cron_pattern ~ '^=' then
                v_jobid := cron.schedule_in_database( job_name => format('%s:%s', p_database_name, p_job_name)
                                                    , schedule => substr(v_cron_pattern, 2)
                                                    , command => format('call cron._cron_job_execution(%L, %L, false, %L);', p_user_name, p_job_name, v_action)
                                                    , database => p_database_name
                                                    , username => p_user_name
                                                    );
            else
                v_jobid := cron.schedule_in_database( job_name => format('%s:%s', p_database_name, p_job_name)
                                                    , schedule => v_cron_pattern
                                                    , command => format('call cron._cron_job_execution(%L, %L, false, %L);', p_user_name, p_job_name, v_action)
                                                    , database => p_database_name
                                                    , username => p_user_name
                                                    );
            end if;
            update cron.job_definition
            set jobid = v_jobid
              , cron_pattern = v_cron_pattern
            where pk = v_derinition_pk;
        end -- _srvr_enable_job
        $body$;
        comment on procedure cron._srvr_enable_job is 'Internal function schedule a job in the cron server database';

        /*
         * procedure _srvr_disable_job
         */
        create procedure _srvr_disable_job
            ( p_database_name        name
            , p_user_name            name
            , p_job_name             varchar(128)
            ) security definer language plpgsql as $body$
        declare
            v_description_pk bigint;
            v_jobid          bigint;
        begin
            select pk, jobid into v_description_pk, v_jobid 
            from cron.job_definition
            where database_name = p_database_name
            and user_name = p_user_name
            and job_name = p_job_name;
            if not found then
                raise exception 'job ''%'' is not known in database ''%'' for user ''%''', p_job_name, p_database_name, p_user_name;
            end if;
            if v_jobid is null then
                raise notice 'job ''%'' is database ''%'' is already disabled', p_job_name, p_database_name;
                return;
            end if;
            
            perform cron.unschedule(v_jobid);
            
            update cron.job_definition
            set cron_pattern = ''
              , jobid = null
            where pk = v_description_pk;
        end -- _srvr_disable_job
        $body$;
        comment on procedure cron._srvr_disable_job is 'Internal function unschedule a job in the cron server database';

        /*
         * procedure _srvr_drop_job
         */
        create procedure _srvr_drop_job
            ( p_database_name        name
            , p_user_name            name
            , p_job_name             varchar(128)
            , p_force                boolean
            ) security definer language plpgsql as $body$
        declare
            v_description_pk bigint;
            v_jobid          bigint;
            v_job_pid        int;
        begin
            select pk, jobid into v_description_pk, v_jobid 
            from cron.job_definition
            where database_name = p_database_name
            and user_name = p_user_name
            and job_name = p_job_name;
            if not found then
                raise notice 'job ''%'' is not known in database ''%'' for user ''%''', p_job_name, p_database_name, p_user_name;
                return;
            end if;
            
            if v_jobid is not null then 
            	perform cron.unschedule(v_jobid);
                if p_force then
                    select job_pid
                    into v_job_pid
                    from cron.job_run_details
                    where jobid = v_jobid
                    order by runid desc
                    limit 1;
                    if found and v_job_pid is not null then
                        perform pg_terminate_backend(job_pid);
                    end if;
                end if;
            end if;
            
            delete from  cron.job_definition
            where pk = v_description_pk;
        end -- _srvr_drop_job
        $body$;
        comment on procedure cron._srvr_drop_job is 'Internal function remove a job from the cron server database';

        /*
         * procedure _srvr_stop_job
         */
        create procedure _srvr_stop_job
            ( p_database_name        name
            , p_user_name            name
            , p_job_name             varchar(128)
            , p_force                boolean
            ) security definer language plpgsql as $body$
        declare
            v_jobid    bigint;
            v_job_pid  int;
        begin
            select jobid into v_jobid 
            from cron.job_definition
            where database_name = p_database_name
            and user_name = p_user_name
            and job_name = p_job_name;
            if not found then
                raise exception 'job ''%'' is not known in database ''%'' for user ''%''', p_job_name, p_database_name, p_user_name;
            end if;
            
            select job_pid
            into v_job_pid
            from cron.job_run_details
            where jobid = v_jobid
            and status = 'running'
            order by runid desc
            limit 1;
            if not found then
        	    raise notice 'job ''%'' in database ''%'' for user ''%'' is not running', p_job_name, p_database_name, p_user_name;
                return;
            end if;
            
            if p_force then
                perform pg_terminate_backend(v_job_pid);
            else
                perform pg_cancel_backend(v_job_pid);
            end if;
        end -- _srvr_stop_job
        $body$;
        comment on procedure cron._srvr_stop_job is 'Internal function to abort a job execution in the cron server database';

        /*
         * procedure _srvr_run_job
         */
        create procedure _srvr_run_job
            ( p_database_name        name
            , p_user_name            name
            , p_job_name             varchar(128)
            ) security definer language plpgsql as $body$
        declare
            v_description_pk      bigint;
            v_jobid               bigint;
            v_action              text;
            v_sql                 text;
        begin
            select pk, jobid, job_action 
              into v_description_pk, v_jobid, v_action 
            from cron.job_definition
            where database_name = p_database_name
            and job_name = p_job_name;
            if not found then
                raise exception 'job ''%'' is not known in database ''%'' for user ''%''', p_job_name, p_database_name, p_user_name;
            end if;
        
            v_sql := format('call cron._cron_job_execution(%L, %L, true, %L)', p_user_name, p_job_name, v_action);
            
            if v_jobid is null then
                v_jobid := cron.schedule_in_database( job_name => format('%s:%s', p_database_name, p_job_name)
                                                    , schedule => '5 seconds'
                                                    , command => v_sql
                                                    , database => p_database_name
                                                    , username => p_user_name
                                                    );
        	    update cron.job_definition
        	       set jobid = v_jobid
        	    where pk = v_description_pk;
        	else
        	    perform cron.alter_job( job_id => v_jobid
        		                      , schedule => '5 seconds'
        		                      , command => v_sql
        		                      );
        	end if;
        end -- _srvr_run_job
        $body$;
        comment on procedure cron._srvr_run_job is 'Internal function to schedule a job in 5 seconds in the cron server database';

        /*
         * procedure _srvr_job_execution_started
         */
        create procedure _srvr_job_execution_started
            ( p_database_name        name
            , p_user_name            name
            , p_job_name             varchar(128)
            , p_once                 boolean
            ) security definer language plpgsql as $body$
        /*
         * 
         */
        declare
            v_description_pk   bigint;
            v_jobid            bigint;
            v_runid            bigint;
            v_status           text := '';
            v_start_date       timestamp with time zone;
            v_end_date         timestamp with time zone;
            v_repeat_interval  text;
            v_cron_pattern     text;
            v_job_action       text;
            v_retry_count      int := 0;
        begin
            select pk, jobid, job_action, start_date, repeat_interval, end_date, cron_pattern
              into v_description_pk, v_jobid, v_job_action, v_start_date, v_repeat_interval, v_end_date, v_cron_pattern 
            from cron.job_definition
            where database_name = p_database_name
            and job_name = p_job_name;
            if not found then
                raise exception 'job ''%'' is not known in database ''%'' for user ''%''', p_job_name, p_database_name, p_user_name;
            end if;

            while v_retry_count < 10 and (not found or v_status != 'running') loop
                v_retry_count := v_retry_count + 1;
                select runid, status into v_runid, v_status
	            from cron.job_run_details
	            where runid = ( select max(runid)
	                            from cron.job_run_details
	                            where jobid = v_jobid
	                          );
				if not found or v_status != 'running' then
                    -- maybe the code starting the job hasn't committed yet
                	perform pg_sleep(v_retry_count);
                end if;
            end loop;
            if not found or v_status != 'running' then
                 raise exception 'Procedure cron.job_execution_started(name, name, varchar, boolean) must be invoked from a running job, status = %', v_status;
            end if;

            insert into cron.job_run (job_definition_pk, job_run_details_runid)
                values (v_description_pk, v_runid);

            if v_cron_pattern = '' then
                perform cron.unschedule(job_id => v_jobid);
                update cron.job_definition
                set cron_pattern = ''
                  , jobid = null
                where pk = v_description_pk;
                return;
            end if;

            if p_once then
                if v_cron_pattern ~ '^=' then
                    perform cron.alter_job( job_id => v_jobid
                                          , schedule => substr(v_cron_pattern, 2)
                                          , command => format('call cron._cron_job_execution(%L, %L, false, %L);', p_user_name, p_job_name, v_job_action)
                                          );
                    return;
                elsif v_cron_pattern ~ '^>' then
        		    perform cron.alter_job( job_id => v_jobid
        		                          , schedule => v_cron_pattern
        		                          , command => format('call cron._cron_job_execution(%L, %L, false, ''select 1;'');',p_user_name, p_job_name)
        		                          );
        		    return;
        		else
                    perform cron.alter_job( job_id => v_jobid
                                          , schedule => v_cron_pattern
                                          , command => format('call cron._cron_job_execution(%L, %L, false, %L);', p_user_name, p_job_name, v_job_action)
                                          );
                end if;
            end if;

            if v_cron_pattern ~ '^=|^>' then
                v_cron_pattern := cron._srvr_make_cron_patten(v_start_date, v_end_date, v_repeat_interval);
                update cron.job_definition set cron_pattern = v_cron_pattern where pk = v_description_pk;
                if v_cron_pattern = '' then
                	perform cron.unschedule(job_id => v_jobid);
        	        update cron.job_definition
        	        set cron_pattern = ''
        	          , jobid = null
        	        where pk = v_description_pk;
                elsif v_cron_pattern ~ '^>' then
                    perform cron.alter_job( job_id => v_jobid
                                          , schedule => substr(v_cron_pattern, 2)
                                          , command => format('call cron._cron_job_execution(%L, %L, false, ''select 1;'');', p_user_name, p_job_name)
                                          );
                elsif v_cron_pattern ~ '^=' then
                    perform cron.alter_job( job_id => v_jobid
                                          , schedule => substr(v_cron_pattern, 2)
                                          , command => format('call cron._cron_job_execution(%L, %L, false, %L);', p_user_name, p_job_name, v_job_action)
                                          );
                else
                    perform cron.alter_job( job_id => v_jobid
                                          , schedule => v_cron_pattern
                                          , command => format('call cron._cron_job_execution(%L, %L, false, %L);', p_user_name, p_job_name, v_job_action)
                                          );
                end if;
            elsif cron._srvr_calculate_next_run_from_cron_pattern(v_cron_pattern, v_end_date) is null then
                v_cron_pattern = '';
                perform cron.unschedule(job_id => v_jobid);
                update cron.job_definition
                set cron_pattern = ''
                  , jobid = null
                where pk = v_description_pk;
            end if;
        end -- _srvr_job_execution_started
        $body$;
        comment on procedure cron._srvr_job_execution_started is 'Internal function to indicate that a job execution has started in the cron server database';

        /*
         * function _srvr_make_cron_patten
         */
        create function _srvr_make_cron_patten
            ( p_start_timestamp timestamp with time zone
            , p_end_timestamp timestamp with time zone
            , p_schedule text
            ) returns text language plpgsql as $body$
        declare
            key_value_pair      text;
            arr                 text[];
            key                 text;
            value               text;
            numeric_value       numeric;
            dow_text            text;
            base_interval       interval := '1 day'::interval;
            cron_interval       interval;
            cron_minute         text := '*';
            cron_hour           text := '*';
            cron_day_of_month   text := '*';
            cron_month          text := '*';
            cron_day_of_week    text := '*';
            freq_unit           text := '';
            nr                  int;
            calculate_next_run  boolean = false;
            timestamp_now       timestamp with time zone;
            next_run_timestamp  timestamp with time zone;
            next_cron_timestamp timestamp;
            new_timestamp       timestamp;
            result_str          text := '';
            local_utc_offset    interval;
            cron_utc_offset     interval;
            tz_delta            int;
            cron_time_zone      text;
            local_time_zone     text;
        begin
            if p_schedule ~ '^\s*([^\s]+\s+){4}[^\s]+\s*$|^\d+\s+seconds$' then
                -- This already is a cron pattern
                if cron._srvr_calculate_next_run_from_cron_pattern(p_schedule, p_end_timestamp) is null then
                    return '';
                end if;
                return p_schedule;
            end if;
            
            foreach key_value_pair in array regexp_split_to_array(p_schedule, '\s*;\s*') loop
                arr := regexp_split_to_array(key_value_pair, '\s*=\s*');
                if cardinality(arr) = 1 and arr[1] = '' then
                    -- an empty entry
                    continue;
                end if;
                if cardinality(arr) != 2 then
                    raise exception 'expecting "%" to be a key=value pair in %', key_value_pair, p_schedule;
                end if;
                key = upper(trim(arr[1]));
                value = upper(trim(arr[2]));
                case key
                when '' then
                    continue;
                when 'FREQ' then
                    freq_unit := value;
                    case value
                    when 'YEARLY' then
                        base_interval := '1 year'::interval;
                        cron_minute := extract(minute from p_start_timestamp);
                        cron_hour := extract(hour from p_start_timestamp);
                        cron_day_of_month := extract(day from p_start_timestamp);
                        cron_month := extract(month from p_start_timestamp);
                    when 'MONTHLY' then
                        base_interval := '1 month'::interval;
                        base_interval := '1 year'::interval;
                        cron_minute := extract(minute from p_start_timestamp);
                        cron_hour := extract(hour from p_start_timestamp);
                        cron_day_of_month := extract(day from p_start_timestamp);
                    when 'WEEKLY' then
                        base_interval := '1 week'::interval;
                        cron_minute := extract(minute from p_start_timestamp);
                        cron_hour := extract(hour from p_start_timestamp);
                        cron_day_of_week := extract(dow from p_start_timestamp);
                    when 'DAILY' then 
                        base_interval := '1 day'::interval;
                        cron_minute := extract(minute from p_start_timestamp);
                        cron_hour := extract(hour from p_start_timestamp);
                    when 'HOURLY' then
                        base_interval := '1 day'::interval;
                        cron_minute := extract(minute from p_start_timestamp);
                    when 'MINUTELY' then
                        base_interval := '1 minute'::interval;
                    when 'SECONDLY' then
                        base_interval := '1 second'::interval;
                    else 
                        raise exception 'Unknown frequency "%" in "%" in "%"', value, key_value_pair, p_schedule;
                    end case;
                when 'BYHOUR' then
                    if base_interval = '1 second'::interval then
                        raise exception 'You can only specify an interval for a secondly frequency in %', p_intput;
                    end if;
                    cron_hour := value;
                when 'BYMINUTE' then
                    if base_interval = '1 second'::interval then
                        raise exception 'You can only specify an interval for a secondly frequency in %', p_intput;
                    end if;
                    cron_minute := value;
                when 'BYSECOND' then
                    if base_interval = '1 second'::interval then
                        raise exception 'You can only specify an interval for a secondly frequency in %', p_intput;
                    end if;
                when 'BYDAY' then
                    if base_interval = '1 second'::interval then
                        raise exception 'You can only specify an interval for a secondly frequency in %', p_intput;
                    end if;
                    cron_day_of_week := '';
                    foreach dow_text in array regexp_split_to_array(value, '\s*,\s*') loop
                        if dow_text = '' then
                            continue;
                        end if;
                        if cron_day_of_week != '' then
                            cron_day_of_week := cron_day_of_week || ',';
                        end if;
                        case 
                        when dow_text ~ '^(0|7|SU|SUN|SUNDAY)$' then
                            cron_day_of_week := cron_day_of_week || '0';
                        when dow_text ~ '^(1|M|MON|MONDAY)$' then
                            cron_day_of_week := cron_day_of_week || '1';
                        when dow_text ~ '^(2|TU|TUE|TUESDAY)$' then
                            cron_day_of_week := cron_day_of_week || '2';
                        when dow_text ~ '^(3|W|WED|WEDNESDAY)$' then
                            cron_day_of_week := cron_day_of_week || '3';
                        when dow_text ~ '^(4|TH|THU|THURSDAY)$' then
                            cron_day_of_week := cron_day_of_week || '4';
                        when dow_text ~ '^(5|F|FRI|FRIDAY)$' then
                            cron_day_of_week := cron_day_of_week || '5';
                        when dow_text ~ '^(6|SA|SAT|SATURDAY)$' then
                            cron_day_of_week := cron_day_of_week || '6';
                        else
                            raise exception 'Unknown day_of_week "%" in "%" in "%"', value, key_value_pair, p_schedule;
                        end case; 
                    end loop;
                when 'BYMONTHDAY' then
                    if base_interval = '1 second'::interval then
                        raise exception 'You can only specify an interval for a secondly frequency in %', p_intput;
                    end if;
                    cron_day_of_month := value;
                when 'INTERVAL' then
                	numeric_value := value::numeric;
                	cron_interval := base_interval * numeric_value;
                    case freq_unit
                    when 'YEARLY' then
                    	calculate_next_run := true;
                    when 'MONTHLY' then
                        if cron_month = '*' 
                        and numeric_value <= 12 and 12 % numeric_value = 0 then
                            cron_month := '';
                            for nr in 0 .. 11 by value::int loop
                                 if  cron_month != '' then
                                     cron_month := cron_month || ',';
                                 end if;
                                 cron_month := cron_month || (extract(month from p_start_timestamp) + nr) % 12;
                            end loop;
                        else
                            calculate_next_run := true;
                        end if;
                    when 'WEEKLY' then
                        calculate_next_run := true;
                    when 'DAILY' then
                        calculate_next_run := true;
                    when 'HOURLY' then
                        calculate_next_run := true;
                    when 'MINUTELY' then
                        if cron_minute = '*'
                        and numeric_value <= 60 and 60 % numeric_value = 0 then
                            cron_minute := '';
                            for nr in 0 .. 59 by numeric_value loop
                                 if  cron_minute != '' then
                                     cron_minute := cron_minute || ',';
                                 end if;
                                 cron_minute := cron_minute || (extract(minute from p_start_timestamp) + nr)::integer % 60;
                            end loop;
                        else
                            calculate_next_run := true;
                        end if;
        	        when 'SECONDLY' then
        	            result_str := value || ' seconds';
                    else
                        calculate_next_run := true;
                    end case;
                else
                    raise exception 'Unknown key "%" in "%" in "%"', key, key_value_pair, p_schedule;
                end case;
            end loop;

            if cron_hour != '*' or cron_minute != '*' then
                select ( select utc_offset
                         from pg_timezone_names tzn
                         join pg_settings st on st.name = 'log_timezone' and setting in (tzn.name, tzn.abbrev)
                         order by case setting when abbrev then 1 else 2 end
                         limit 1
                       ) local_offset
                     , ( select utc_offset
                         from pg_timezone_names tzn
                         join pg_settings st on st.name = 'cron.timezone' and setting in (tzn.name, tzn.abbrev)
                         order by case setting when abbrev then 1 else 2 end
                         limit 1) cron_offset
                into  local_utc_offset, cron_utc_offset;
                if cron_hour != '*' then
                    tz_delta = extract(hours from local_utc_offset - cron_utc_offset);
                    if tz_delta != 0 then
                        arr = regexp_split_to_array(cron_hour, ',');
                        for nr in 1 .. cardinality(arr) loop
                            arr[nr] := ((arr[nr]::int + 24 - tz_delta) % 24)::text;
                        end loop;
                        cron_hour := array_to_string(arr, ',');
                    end if; 
                end if;
                if cron_minute != '*' then
                    tz_delta = extract(minutes from local_utc_offset - cron_utc_offset);
                    if tz_delta != 0 then
                        arr = regexp_split_to_array(cron_minute, ',');
                        for nr in 1 .. cardinality(arr) loop
                            arr[nr] := ((arr[nr]::int + 60 - tz_delta) % 60)::text;
                        end loop;
                        cron_minute := array_to_string(arr, ','); 
                    end if;
                end if;
            end if;
                
            timestamp_now := clock_timestamp();
            if timestamp_now <= p_start_timestamp then
                calculate_next_run = true;
                next_run_timestamp := p_start_timestamp;
            else
                next_run_timestamp := timestamp_now + interval '1 minute';
            end if;

            if calculate_next_run then
                if cron_interval is null then
                    cron_interval = base_interval;
                end if;
                if next_run_timestamp > p_start_timestamp then    
                    nr = ceil(extract(epoch from next_run_timestamp - p_start_timestamp) / extract(epoch from cron_interval));
                    next_run_timestamp := p_start_timestamp + nr * cron_interval;
                    loop                   -- intervals >= a day, week and month may differ in nr of seconds
                        if next_run_timestamp > timestamp_now then
                            exit;
                        end if;
                        next_run_timestamp := next_run_timestamp + cron_interval;
                    end loop;
                    loop                   -- intervals >= a day, week and month may differ in nr of seconds
                        if next_run_timestamp < timestamp_now + cron_interval then
                            exit;
                        end if;
                        next_run_timestamp := next_run_timestamp - cron_interval;
                    end loop;
                end if;
                select setting into cron_time_zone from pg_settings where name = 'cron.timezone';
                select setting into local_time_zone from pg_settings where name = 'log_timezone';
                next_cron_timestamp = next_run_timestamp at time zone cron_time_zone;
                new_timestamp = next_cron_timestamp;
                loop
                    if p_end_timestamp is not null and next_cron_timestamp > p_end_timestamp at time zone cron_time_zone then
                        next_cron_timestamp = null;
                        exit;
                    end if;
                    new_timestamp = cron._srvr_first_valid_month(cron_month, next_cron_timestamp);
                    if next_cron_timestamp != new_timestamp then
                        next_cron_timestamp = new_timestamp;
                        continue;
                    end if;
                    new_timestamp = cron._srvr_first_valid_day_of_month(cron_day_of_month, next_cron_timestamp);
                    if next_cron_timestamp != new_timestamp then
                        next_cron_timestamp = new_timestamp;
                        continue;
                    end if;
                    new_timestamp = cron._srvr_first_valid_day_of_week(cron_day_of_week, next_cron_timestamp);
                    if next_cron_timestamp != new_timestamp then
                        next_cron_timestamp = new_timestamp;
                        continue;
                    end if;
                    new_timestamp = cron._srvr_first_valid_hour(cron_hour, next_cron_timestamp);
                    if next_cron_timestamp != new_timestamp then
                        next_cron_timestamp = new_timestamp;
                        continue;
                    end if;
                    new_timestamp = cron._srvr_first_valid_minute(cron_minute, next_cron_timestamp);
                    if next_cron_timestamp != new_timestamp then
                        next_cron_timestamp = new_timestamp;
                        continue;
                    end if;
                    exit;
                end loop;
                if next_cron_timestamp is null then
                    return '';
                end if;
                next_run_timestamp = next_cron_timestamp at time zone cron_time_zone at time zone local_time_zone;
                result_str := '=' || cron._srvr_cron_at_timestamp(next_run_timestamp);
            else
	            if result_str = '' then
	                result_str := format('%s %s %s %s %s', cron_minute, cron_hour, cron_day_of_month, cron_month, cron_day_of_week);
                end if;
                if cron._srvr_calculate_next_run_from_cron_pattern(result_str, p_end_timestamp) is null then
                    return '';
                end if;
            end if;
            return result_str;
        end -- _srvr_make_cron_patten
        $body$;
        comment on function cron._srvr_make_cron_patten is 'Internal function to calculate a cron pattern from a scheduler pattern in the cron server database';

        /*
         * function _srvr_cron_at_timestamp
         */
        create function _srvr_cron_at_timestamp(p_at_timestamp timestamp with time zone) returns text language plpgsql as $body$
        declare
            v_time_zone           text;
            v_schedule_timestamp  timestamp with time zone;
            v_crontab_string      text;
        begin
            select setting into v_time_zone from pg_settings where name = 'cron.timezone';
            v_schedule_timestamp = p_at_timestamp at time zone v_time_zone;
            return format( '%s %s %s %s %s'
                         , extract(minutes from v_schedule_timestamp)
                         , extract(hours from v_schedule_timestamp)
                         , extract(day from v_schedule_timestamp)
                         , extract(month from v_schedule_timestamp)
                         , extract(dow from v_schedule_timestamp)
                         );
        end
        $body$; -- _srvr_cron_at_timestamp
        comment on function cron._srvr_cron_at_timestamp is 'Internal function to calculate a cron pattern to run at a given timestamp in the cron server database';

        /*
         * function _srvr_calculate_next_run_from_cron_pattern
         */
        create function _srvr_calculate_next_run_from_cron_pattern
            ( cron_pattern    text
            , p_end_timestamp  timestamp with time zone
            ) returns timestamp with time zone  language plpgsql as $body$
        declare
            next_run_timestamp  timestamp;
            arr                 text[];
            cron_minute         text := '*';
            cron_hour           text := '*';
            cron_day_of_month   text := '*';
            cron_month          text := '*';
            cron_day_of_week    text := '*';
            new_timestamp       timestamp;
            cron_time_zone      text;
            local_time_zone     text;
        begin
            select setting into cron_time_zone from pg_settings where name = 'cron.timezone';
            select setting into local_time_zone from pg_settings where name = 'log_timezone';
            if cron_pattern ~ '^\d+\s+seconds$' then
                next_run_timestamp = clock_timestamp() + cron_pattern::interval;
                if p_end_timestamp is not null and next_run_timestamp > p_end_timestamp then
                   next_run_timestamp = null;
               end if;
               return next_run_timestamp;
            elsif cron_pattern ~ '^=\s*(\d+\s+){4}\d+' then
                arr := regexp_split_to_array(replace(cron_pattern, '=', ''), '\s+');
                next_run_timestamp := format( '%s-%s-%s %s:%s'
                                            , extract(year from clock_timestamp())
                                            , arr[4]
                                            , arr[3]
                                            , arr[2]
                                            , arr[1]
                                            )::timestamp with time zone;
                if next_run_timestamp > p_end_timestamp at time zone cron_time_zone then
                    next_run_timestamp := null;
                end if;
                return next_run_timestamp;
            elsif not cron_pattern ~ '^>?([\d*,]+\s+){4}[\d*,]+$' then
                raise exception '% is not a legal cron pattern', cron_pattern;
            end if;
             
            next_run_timestamp := date_trunc('minute', clock_timestamp() at time zone cron_time_zone + interval '1 minute');
            arr := regexp_split_to_array(trim(replace(cron_pattern, '>', '')), '\s+');
            cron_minute := arr[1];
            cron_hour := arr[2];
            cron_day_of_month := arr[3];
            cron_month := arr[4];
            cron_day_of_week := arr[5];
            
            new_timestamp = next_run_timestamp;
            loop
                if p_end_timestamp is not null and next_run_timestamp > p_end_timestamp at time zone cron_time_zone then
                    next_run_timestamp = null;
                    exit;
                end if;
                new_timestamp = cron._srvr_first_valid_month(cron_month, next_run_timestamp);
                if next_run_timestamp != new_timestamp then
                    next_run_timestamp = new_timestamp;
                    continue;
                 end if;
                 new_timestamp = cron._srvr_first_valid_day_of_month(cron_day_of_month, next_run_timestamp);
                 if next_run_timestamp != new_timestamp then
                     next_run_timestamp = new_timestamp;
                     continue;
                 end if;
                 new_timestamp = cron._srvr_first_valid_day_of_week(cron_day_of_week, next_run_timestamp);
                 if next_run_timestamp != new_timestamp then
                     next_run_timestamp = new_timestamp;
                     continue;
                 end if;
                 new_timestamp = cron._srvr_first_valid_hour(cron_hour, next_run_timestamp);
                 if next_run_timestamp != new_timestamp then
                     next_run_timestamp = new_timestamp;
                     continue;
                 end if;
                 new_timestamp = cron._srvr_first_valid_minute(cron_minute, next_run_timestamp);
                 if next_run_timestamp != new_timestamp then
                     next_run_timestamp = new_timestamp;
                     continue;
                 end if;
                 exit;
            end loop;
            return next_run_timestamp at time zone cron_time_zone at time zone local_time_zone;
        end -- _srvr_calculate_next_run_from_cron_pattern
        $body$;
        comment on function cron._srvr_calculate_next_run_from_cron_pattern is 'Internal function to calculate the next run timestamp from a given cron pattern in the cron server database';

        /*
         * function _srvr_first_valid_month
         */
        create function _srvr_first_valid_month(month_pattern text, run_timestamp timestamp) returns timestamp language plpgsql as $body$
        declare
            arr               text[];
            result_timestamp  timestamp := run_timestamp;
        begin
            if month_pattern != '*' then
                arr = regexp_split_to_array(month_pattern, ',');
                if not extract(month from result_timestamp)::text = any (arr) then
                    loop
                        if extract(month from result_timestamp)::text = any (arr) then
                            exit;
                        end if;
                        result_timestamp := result_timestamp + interval '1 month';
                    end loop;
                    result_timestamp := date_trunc('month', result_timestamp);
                end if;
            end if;
            return result_timestamp;
        end -- _srvr_first_valid_month
        $body$;
        comment on function cron._srvr_first_valid_month is 'Internal function to validate a cron pattern in the cron server database';

        /*
         * function _srvr_first_valid_day_of_month
         */
        create function _srvr_first_valid_day_of_month(day_of_month_pattern text, run_timestamp timestamp) returns timestamp language plpgsql as $body$
        declare
            arr               text[];
            result_timestamp  timestamp := run_timestamp;
        begin
            if day_of_month_pattern != '*' then
        	    arr = regexp_split_to_array(day_of_month_pattern, ',');
        	    if not extract(day from result_timestamp)::text = any (arr) then
        	        loop
        	            if extract(day from result_timestamp)::text = any (arr) then
        	                exit;
        	            end if;
        	            result_timestamp := result_timestamp + interval '1 day';
        	        end loop;
        	        result_timestamp := date_trunc('day', result_timestamp);
        	    end if;
        	end if;
            return result_timestamp;
        end -- _srvr_first_valid_day_of_month
        $body$;
        comment on function cron._srvr_first_valid_day_of_month is 'Internal function to validate a cron pattern in the cron server database';

        /*
         * function _srvr_first_valid_day_of_week
         */
         create function _srvr_first_valid_day_of_week(day_of_week_pattern text, run_timestamp timestamp) returns timestamp language plpgsql as $body$
         declare
             arr               text[];
             result_timestamp  timestamp := run_timestamp;
         begin
             if day_of_week_pattern != '*' then
        	    arr = regexp_split_to_array(day_of_week_pattern, ',');
        	    if not extract(dow from result_timestamp)::text = any (arr) then
        	        loop
        	            if extract(dow from result_timestamp)::text = any (arr) then
        	                exit;
        	            end if;
        	            result_timestamp := result_timestamp + interval '1 day';
        	        end loop;
        	        result_timestamp := date_trunc('day', result_timestamp);
        	    end if;
        	 end if;
        	 return result_timestamp;
         end -- _srvr_first_valid_day_of_week
         $body$;
        comment on function cron._srvr_first_valid_day_of_week is 'Internal function to validate a cron pattern in the cron server database';

        /*
         * function _srvr_first_valid_hour
         */
         create function _srvr_first_valid_hour(hour_pattern text, run_timestamp timestamp) returns timestamp language plpgsql as $body$
         declare
             arr               text[];
             result_timestamp  timestamp := run_timestamp;
         begin
             if hour_pattern != '*' then
        	    arr = regexp_split_to_array(hour_pattern, ',');
        	    if not extract(hour from result_timestamp)::text = any (arr) then
        	        loop
        	            if extract(hour from result_timestamp)::text = any (arr) then
        	                exit;
        	            end if;
        	            result_timestamp := result_timestamp + interval '1 hour';
        	        end loop;
        	        result_timestamp := date_trunc('hour', result_timestamp);
        	    end if;
        	 end if;
        	 return result_timestamp;
         end -- _srvr_first_valid_hour
         $body$;
        comment on function cron._srvr_first_valid_hour is 'Internal function to validate a cron pattern in the cron server database';

        /*
         * function _srvr_first_valid_minute
         */
         create function _srvr_first_valid_minute(minute_pattern text, run_timestamp timestamp) returns timestamp language plpgsql as $body$
         declare
             arr               text[];
             result_timestamp  timestamp := run_timestamp;
         begin
             if minute_pattern != '*' then
        	    arr = regexp_split_to_array(minute_pattern, ',');
        	    if not extract(minute from result_timestamp)::text = any (arr) then
        	        loop
        	            if extract(minute from result_timestamp)::text = any (arr) then
        	                exit;
        	            end if;
        	            result_timestamp := result_timestamp + interval '1 minute';
        	        end loop;
        	        result_timestamp := date_trunc('minute', result_timestamp);
        	    end if;
        	 end if;
        	 return result_timestamp;
         end -- _srvr_first_valid_minute
         $body$;
        comment on function cron._srvr_first_valid_minute is 'Internal function to validate a cron pattern in the cron server database';

        /*
         * function _srvr_list_jobs
         */
         create function _srvr_list_jobs(p_database_name name)
            returns setof json
            security definer language plpgsql as $body$
        declare
             rec            record;
        begin
            for rec in select json_build_object( 'job_name'        , job_name
                                               , 'job_action'      , job_action
                                               ,  'start_date'     , start_date
                                               , 'repeat_interval' , repeat_interval
                                               , 'end_date'        , end_date
                                               , 'enabled'         , cron_pattern != ''
                                               , 'auto_drop'       , auto_drop
                                               , 'user_name'       , user_name
                                               , 'comments'        , comments
                                               ) as content
                       from cron.job_definition
                       where database_name = p_database_name
            loop
                return next rec.content;
            end loop;
        end -- _srvr_list_jobs
        $body$;
        comment on function cron._srvr_list_jobs is 'Internal function to list existing jobs in the cron server database';

		/*
		 * function _srvr_get_job_state
		 */
		create function _srvr_get_job_state
		    ( p_database_name     name
		    , p_user_name         name
		    , p_job_name          varchar(128)
		    ) returns text
		    security definer language plpgsql as $body$
		declare
		     result_str           text;
		begin
		    select coalesce(det.status, case when def.jobid is null then 'defined' else 'scheduled' end)
		      into result_str 
            from cron.job_definition def
               , lateral (select max(job_run_details_runid) runid from cron.job_run where job_definition_pk = def.pk) run
            left join cron.job_run_details det on det.runid = run.runid
            where def.database_name = p_database_name
              and def.user_name = p_user_name
              and def.job_name = p_job_name;
            if not found then
                result_str := 'unknown';
            end if;
            return result_str;
		end;  -- _srvr_get_job_state
		$body$;
		comment on function cron._srvr_get_job_state is 'Internal function to select a job''s state in the cron server database';
    else
        /*
         * This part only goes into a pg_cron client database - the database in which pg_cron is NOT installed
         */
         result_str := public.dblink_connect('cron_server', 'cron_ctrl_server');
         if result_str = 'ERROR' then
             raise exception $$public.dblink_connect('cron_server', 'cron_ctrl_server')' returned %$$, result_str;
         end if;
         begin
             sql := e'select exists(select * from pg_extension where extname = \'pg_cron_helper\')::text val';
             result_str := public.dblink_open('cron_server', 'cur', sql);
             if result_str = 'ERROR' then
                 raise exception $$public.dblink_open('cron_server', 'cur', %) returned %$$, sql, result_str;
             end if;     
             select val into cron_server_present from public.dblink_fetch('cron_server', 'cur', 1) as (val text);
         exception when others then
             result_str := public.dblink_disconnect('cron_server');
             if result_str = 'ERROR' then
                 raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
              end if;
              raise;
         end;
         result_str := public.dblink_disconnect('cron_server');
         if result_str = 'ERROR' then
             raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
         end if;
         if cron_server_present != 'true' then
             raise exception 'The pg_cron and pg_cron_helper extensions need to be installed first on the database that contains the pg_cron extension';
         end if;
    end if;
end
$do$;

/*
 * procedure create_job
 */
create procedure create_job
    ( job_name             varchar(128)
    , job_action           text
    , start_date           timestamp with time zone  default current_timestamp::timestamp(0) with time zone
    , repeat_interval      varchar                   default ''
    , end_date             timestamp with time zone  default null
    , enabled              boolean                   default false
    , auto_drop            boolean                   default true
    , comments             text                      default ''
    , user_name            name                      default current_user
    ) security definer language plpgsql as $body$
declare
     result_str           text;
     sql                  text;
begin    
     result_str := public.dblink_connect('cron_server', 'cron_ctrl_server');
     if result_str = 'ERROR' then
         raise exception $$public.dblink_connect('cron_server', 'cron_ctrl_server')' returned %$$, result_str;
     end if;
     begin
         sql := format( 'call cron._srvr_create_job(%L, %L, %L, %L, %L, %L, %L, %L, %L, %L)'
                      , current_database()
                      , user_name
                      , job_name
                      , job_action
                      , start_date
                      , repeat_interval
                      , end_date
                      , enabled
                      , auto_drop
                      , comments
                      );
         result_str := public.dblink_exec('cron_server', sql);
         if result_str = 'ERROR' then
             raise exception $$public.dblink_exec('cron_server', '%') returned %$$, sql, result_str;
         end if;
     exception when others then
         result_str := public.dblink_disconnect('cron_server');
         if result_str = 'ERROR' then
             raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
         end if;
         raise;
     end;      
     result_str := public.dblink_disconnect('cron_server');
     if result_str = 'ERROR' then
         raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
     end if;
end
$body$;
comment on procedure cron.create_job is 'Defines a new job
procedure cron.create_job
    ( job_name         => Name of the job. Must be unique (within user_name)
    , job_action       => Sql script that is to be executed
    , start_date       => When to start the repeat interval
    , repeat_interval  => Cron pattern or Oracle style run pattern
    , end_date         => When to stop the repeat interval
    , enabled          => Must the repeat interval be enabled
    , auto_drop        => Not functional yet
    , comments         => Just comment
    , user_name        => Who defined the job
    )';
    
/*
 * procedure enable_job
 */
create procedure enable_job
    ( job_name             varchar(128)
    , user_name            name default current_user
    ) security definer language plpgsql as $body$
declare
     result_str           text;
     sql                  text;
begin    
    result_str := public.dblink_connect('cron_server', 'cron_ctrl_server');
    if result_str = 'ERROR' then
        raise exception $$public.dblink_connect('cron_server', 'cron_ctrl_server')' returned %$$, result_str;
    end if;
    begin
        sql := format( 'call cron._srvr_enable_job(%L, %L, %L)'
                     , current_database()
                     , user_name
                     , job_name
                     );
        result_str := public.dblink_exec('cron_server', sql);
        if result_str = 'ERROR' then
            raise exception $$public.dblink_exec('cron_server', '%') returned %$$, sql, result_str;
        end if;
    exception when others then   
        result_str := public.dblink_disconnect('cron_server');
        if result_str = 'ERROR' then
            raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
        end if;
        raise;
    end;
    result_str := public.dblink_disconnect('cron_server');
    if result_str = 'ERROR' then
        raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
    end if;
end
$body$;
comment on procedure cron.enable_job is 'Makes sure the job is scheduled.
procedure cron.enable_job
    ( job_name         => Name of the job to be enabled
    , user_name        => The user name 
    )';
    
/*
 * procedure disable_job
 */
create procedure disable_job
    ( job_name             varchar(128)
    , user_name            name default current_user
    ) security definer language plpgsql as $body$
declare
     result_str           text;
     sql                  text;
begin    
    result_str := public.dblink_connect('cron_server', 'cron_ctrl_server');
    if result_str = 'ERROR' then
        raise exception $$public.dblink_connect('cron_server', 'cron_ctrl_server')' returned %$$, result_str;
    end if;
    begin
        sql := format( 'call cron._srvr_disable_job(%L, %L, %L)'
                     , current_database()
                     , user_name
                     , job_name
                     );
        result_str := public.dblink_exec('cron_server', sql);
        if result_str = 'ERROR' then
            raise exception $$public.dblink_exec('cron_server', '%') returned %$$, sql, result_str;
        end if;
    exception when others then   
        result_str := public.dblink_disconnect('cron_server');
        if result_str = 'ERROR' then
            raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
        end if;
        raise;
    end;
    result_str := public.dblink_disconnect('cron_server');
    if result_str = 'ERROR' then
        raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
    end if;
end
$body$;
comment on procedure cron.disable_job is 'Removes the job from the schedule
procedure cron.disable_job
    ( job_name         => Name of the job to be enabled
    , user_name        => For which user
    )';

/*
 * procedure stop_job
 */
create procedure stop_job
    ( job_name             varchar(128)
    , force                boolean      default false
    , user_name            name         default current_user
    ) security definer language plpgsql as $body$
declare
     result_str           text;
     sql                  text;
begin    
     result_str := public.dblink_connect('cron_server', 'cron_ctrl_server');
     if result_str = 'ERROR' then
         raise exception $$public.dblink_connect('cron_server', 'cron_ctrl_server')' returned %$$, result_str;
     end if;
     begin
         sql := format( 'call cron._srvr_stop_job(%L, %L, %L, %L)'
                      , current_database()
                      , user_name
                      , job_name
                      , force
                      ); 
         result_str := public.dblink_exec('cron_server', sql);
         if result_str = 'ERROR' then
             raise exception $$public.dblink_exec('cron_server', '%') returned %$$, sql, result_str;
         end if;
     exception when others then     
         result_str := public.dblink_disconnect('cron_server');
         if result_str = 'ERROR' then
             raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
         end if;
         raise;
     end;
     result_str := public.dblink_disconnect('cron_server');
     if result_str = 'ERROR' then
         raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
     end if;
end
$body$;
comment on procedure cron.stop_job is 'Stops (aborts) the execution of a job if it is running
create procedure cron.stop_job
    ( job_name         => Name of the job to be aborted
    , force            => true uses pg_terminate_backend(), false uses pg_cancel_backend()
    , user_name        => For which user
    )';

/*
 * procedure run_job
 */
create procedure run_job
    ( job_name             varchar(128)
    , user_name            name default current_user
    ) security definer language plpgsql as $body$
declare
     result_str           text;
     sql                  text;
begin    
     result_str := public.dblink_connect('cron_server', 'cron_ctrl_server');
     if result_str = 'ERROR' then
         raise exception $$public.dblink_connect('cron_server', 'cron_ctrl_server')' returned %$$, result_str;
     end if;
     begin
         sql := format( 'call cron._srvr_run_job(%L, %L, %L)'
                      , current_database()
                      , user_name
                      , job_name
                      );
         result_str := public.dblink_exec('cron_server', sql);
         if result_str = 'ERROR' then
             raise exception $$public.dblink_exec('cron_server', '%') returned %$$, sql, result_str;
         end if;
     exception when others then     
         result_str := public.dblink_disconnect('cron_server');
         if result_str = 'ERROR' then
             raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
         end if;
         raise;
     end;
     result_str := public.dblink_disconnect('cron_server');
     if result_str = 'ERROR' then
         raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
     end if;
end
$body$;
comment on procedure cron.run_job is 'Starts the execution of the job within 5 seconds, regardless if the job is enabled or not
procedure cron.run_job
    ( job_name         => Name of the job to be started "now"
    , user_name        => "owner" of the job
    )';

/*
 * procedure drop_job
 */
create procedure drop_job
    ( job_name             varchar(128)
    , force                boolean default false
    , user_name            name default current_user
    ) security definer language plpgsql as $body$
declare
     result_str           text;
     sql                  text;
begin    
    result_str := public.dblink_connect('cron_server', 'cron_ctrl_server');
    if result_str = 'ERROR' then
        raise exception $$public.dblink_connect('cron_server', 'cron_ctrl_server')' returned %$$, result_str;
    end if;
    begin
        sql := format( 'call cron._srvr_drop_job(%L, %L, %L, %L)'
                     , current_database()
                     , user_name
                     , job_name
                     , force
                     );
        result_str := public.dblink_exec('cron_server', sql);
        if result_str = 'ERROR' then
            raise exception $$public.dblink_exec('cron_server', '%') returned %$$, sql, result_str;
        end if;
    exception when others then   
        result_str := public.dblink_disconnect('cron_server');
        if result_str = 'ERROR' then
            raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
        end if;
        raise;
    end;
    result_str := public.dblink_disconnect('cron_server');
    if result_str = 'ERROR' then
        raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
    end if;
end
$body$;
comment on procedure cron.drop_job is 'Deletes the job definition
procedure cron.drop_job
    ( job_name         => Name of the job to be started "now"
    , force            => if the job is currently running then perfomr pg_terminate_backend() to stop it
    , user_name        => User for which the job was defined
    )';

/*
 * procedure _cron_job_execution
 */
create procedure _cron_job_execution
    ( user_name name
    , job_name varchar(128)
    , once     boolean
    , task     text
    ) security definer language plpgsql as $body$
declare
     result_str           text;
     sql                  text;
begin    
    result_str := public.dblink_connect('cron_server', 'cron_ctrl_server');
    if result_str = 'ERROR' then
        raise exception $$public.dblink_connect('cron_server', 'cron_ctrl_server')' returned %$$, result_str;
    end if;
    begin
        sql := 'begin';
        result_str := public.dblink_exec('cron_server', sql);
        if result_str = 'ERROR' then
            raise exception $$public.dblink_exec('cron_server', '%') returned %$$, sql, result_str;
        end if;
        sql := format( 'call cron._srvr_job_execution_started(%L, %L, %L, %L)'
                     , current_database()
                     , user_name
                     , job_name
                     , once
                     );
        result_str := public.dblink_exec('cron_server', sql);
        if result_str = 'ERROR' then
            raise exception $$public.dblink_exec('cron_server', '%') returned %$$, sql, result_str;
        end if;
        sql := 'commit';
        result_str := public.dblink_exec('cron_server', sql);
        if result_str = 'ERROR' then
            raise exception $$public.dblink_exec('cron_server', '%') returned %$$, sql, result_str;
        end if;
    exception when others then     
        result_str := public.dblink_disconnect('cron_server');
        if result_str = 'ERROR' then
            raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
        end if;
        raise;
    end;
    result_str := public.dblink_disconnect('cron_server');
    if result_str = 'ERROR' then
        raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
    end if;
    
    execute task;
end
$body$;
comment on procedure cron._cron_job_execution is 'Internal procedure to inform the cron server that a job has started';

/*
 * type job_record
 */
create type job_record as
    ( job_name             varchar(128) 
    , job_action           text
    , start_date           timestamp with time zone
    , repeat_interval      text
    , end_date             timestamp with time zone
    , enabled              boolean
    , auto_drop            boolean
    , user_name            name
    , comments             text
    );

/*
 * function list_jobs
 */
create function list_jobs()
    returns setof cron.job_record
    security definer language plpgsql as $body$
declare
     result_str           text;
     sql                  text;
     rec                  record;
begin
    result_str := public.dblink_connect('cron_server', 'cron_ctrl_server');
    if result_str = 'ERROR' then
        raise exception $$public.dblink_connect('cron_server', 'cron_ctrl_server')' returned %$$, result_str;
    end if;
    begin
        sql := format( 'select * from cron._srvr_list_jobs(%L)'
                     , current_database()
                     );
        result_str := public.dblink_open('cron_server', 'cur', sql);
        if result_str = 'ERROR' then
            raise exception $$public.dblink_exec('cron_server', '%') returned %$$, sql, result_str;
        end if;
        for rec in select content
                   from public.dblink_fetch('cron_server', 'cur', 10000) as f(content json )
        loop
            return next json_populate_record(null::cron.job_record, rec.content);
        end loop;
    exception when others then
        result_str := public.dblink_disconnect('cron_server');
        if result_str = 'ERROR' then
            raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
        end if;
        raise;
    end;
    result_str := public.dblink_disconnect('cron_server');
    if result_str = 'ERROR' then
        raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
    end if;
end
$body$;
comment on function cron.list_jobs is 'Shows the peculiarities of all defined jobs
function cron.list_jobs()
    returns setof cron.job_record';

/*
 * function get_job_state
 */
create function get_job_state
    ( job_name             varchar(128)
    , user_name            name default current_user
    ) returns text
    security definer language plpgsql as $body$
declare
     result_str           text;
     sql                  text;
     function_result      text;
begin    
    result_str := public.dblink_connect('cron_server', 'cron_ctrl_server');
    if result_str = 'ERROR' then
        raise exception $$public.dblink_connect('cron_server', 'cron_ctrl_server')' returned %$$, result_str;
    end if;
    begin
        sql := format( 'select cron._srvr_get_job_state(%L, %L, %L)'
                     , current_database()
                     , user_name
                     , job_name
                     );
        select job_state into function_result from public.dblink('cron_server', sql) f(job_state text);
    exception when others then   
        result_str := public.dblink_disconnect('cron_server');
        if result_str = 'ERROR' then
            raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
        end if;
        raise;
    end;
    result_str := public.dblink_disconnect('cron_server');
    if result_str = 'ERROR' then
        raise exception $$public.dblink_disconnect('cron_server') returned %$$, result_str;
    end if;
    return function_result;
end
$body$;
comment on function cron.get_job_state is $$Returns the state of the specified job
function cron.get_job_state
    ( job_name         => Name of the job to be started "now"
    , user_name        => User name for whom the job is defined
    ) returns text     => one of:
              'defined'   => job is defined but not scheduled
              'scheduled' => job is scheduled, no previous run known
              'running'   => job is currently active
              'succeeded' => the last run was succesful
              'failed'    => the last run was unsuccesful$$;