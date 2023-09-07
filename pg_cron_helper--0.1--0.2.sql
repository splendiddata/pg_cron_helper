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

        /*
         * Procedure   : _srvr_job_execution_started
         * Description : Is invoked by procedure _cron_job_execution, via dblink, in a transaction of its own, to indicate that
         *               the execution of a job has started. It is intended to check the schedule, an re-schedule if necessary.
         * Arguments   :
         *    p_database_name   : Name of the database in which the job is running. This is part of the unique identification of the job.
         *    p_user_name       : The user name for which the job is running. This is also a pert of the unique identification of the job.
         *    p_job_name        : The last part to uniquely identify the job
         *    p_once            : If true, the job is running outside its normal schedule, probably because of a job_run() invocation.
         *                        The job has to be rescheduled.
         *
         * The cron.job_definition table is interogated to check the cron pattern. If that starts with an euqals sign (=) or a
         * greater thansign (>), then a new cron schedule will be calculated for the next run.
         * The next start timestamp will be calculated and if that is beyond the job's end data, the job will be unscheduled.
         */
        create or replace procedure _srvr_job_execution_started
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
            v_not_found        boolean := true;
        begin
            select pk, jobid, job_action, start_date, repeat_interval, end_date, cron_pattern
              into v_description_pk, v_jobid, v_job_action, v_start_date, v_repeat_interval, v_end_date, v_cron_pattern
            from cron.job_definition
            where database_name = p_database_name
            and job_name = p_job_name;
            if not found then
                raise exception 'job ''%'' is not known in database ''%'' for user ''%''', p_job_name, p_database_name, p_user_name;
            end if;

            while v_retry_count < 10 and (v_not_found or coalesce(v_status, 'null') != 'running') loop
                v_retry_count := v_retry_count + 1;
                select runid, status into v_runid, v_status
	            from cron.job_run_details
	            where runid = ( select max(runid)
	                            from cron.job_run_details
	                            where jobid = v_jobid
	                          );
                v_not_found := not found;
				if v_not_found or coalesce(v_status, 'null') != 'running' then
                    -- maybe the code starting the job hasn't committed yet
                	perform pg_sleep(v_retry_count);
                end if;
            end loop;
            if v_not_found or coalesce(v_status, 'null') != 'running' then
                 raise exception 'Procedure cron.job_execution_started(name, name, varchar, boolean) must be invoked from a running job, status = %', coalesce(v_status, 'null');
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

    end if;
end
$do$;