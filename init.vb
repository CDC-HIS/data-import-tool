
       mssql = "SELECT Setting.Value AS HMISCode, Facility.FacilityName, Province.Name AS Region, District.Name As Woreda FROM Setting INNER JOIN Facility ON Setting.Value = Facility.HMISCode INNER JOIN District ON Facility.DistrictId = District.DistrictSeq INNER JOIN Province ON District.ProvinceSeq = Province.ProvinceSeq WHERE     (Setting.Name = 'HmisCode')"
        
      
mssql = "select Count(*) as total,fb.follow_up_status from (" & _
 "select  f.Id as fid, f.PatientId,f.art_dose_end,f.FollowUpDate,c.Id, case when c.Id is null then 'Not counted' else'counted' end as expr" & _
" from ( SELECT c.Id,c.PatientID,c.art_start_date AS ARTstartedDate, c.[follow_up_status],c.art_dose_end,c.FollowUpDate" & _
" FROM dbo.Registration AS reg INNER JOIN (SELECT d.Id,d.art_dose_end,d.PatientId, d.FollowUpDate, d.follow_up_status, d.art_start_date, d.next_visit_date" & _
" FROM dbo.crtEthiopiaARTVisit AS d INNER JOIN (SELECT MAX(v.Id) AS Id FROM dbo.crtEthiopiaARTVisit AS v INNER JOIN" & _
" (SELECT PatientId, MAX(FollowUpDate) AS FollowupDate" & _
" FROM dbo.crtEthiopiaARTVisit WHERE      (Deprecated = 0) AND (NOT (follow_up_status IS NULL)) AND (art_start = 1)" & _
" and FollowUpDate <= '" & sDate & "' GROUP BY PatientId) AS l ON v.PatientId = l.PatientId AND v.FollowUpDate = l.FollowupDate" & _
" GROUP BY v.PatientId, v.FollowUpDate, v.Deprecated HAVING      (v.Deprecated = 0)) AS lf ON d.Id = lf.Id) AS c ON c.PatientId = reg.PId" & _
" where c.follow_up_status >4) as f" & _
" OUTER APPLY(" & _
" SELECT d.Id,d.art_dose_end,d.PatientId, d.FollowUpDate, d.follow_up_status, d.UniqueArtNumber,d.art_start_date,  d.next_visit_date" & _
" FROM dbo.crtEthiopiaARTVisit AS d INNER JOIN(SELECT     MAX(v.Id) AS Id" & _
" FROM dbo.crtEthiopiaARTVisit AS v INNER JOIN (SELECT     PatientId, MAX(FollowUpDate) AS FollowupDate" & _
" FROM dbo.crtEthiopiaARTVisit WHERE (Deprecated = 0) AND (NOT (follow_up_status IS NULL)) AND (art_start = 1)" & _
" and FollowUpDate <= '" & eDate & "' GROUP BY PatientId) AS l ON v.PatientId = l.PatientId AND v.FollowUpDate = l.FollowupDate" & _
" GROUP BY v.PatientId, v.FollowUpDate, v.Deprecated HAVING      (v.Deprecated = 0)) AS lf ON d.Id = lf.Id where d.follow_up_status >4" & _
" and f.PatientId=d.PatientId and d.art_start_date <= '" & eDate & "' and  d.FollowUpDate <= '" & eDate & "' and  d.art_dose_end >= '" & eDate & "'" & _
" ) as c where   f.ARTstartedDate <= '" & sDate & "' and  f.FollowUpDate <= '" & sDate & "' and  f.art_dose_end >= '" & sDate & "' and c.Id is null) as n" & _
" inner join (" & _
" SELECT d.Id,d.art_dose_end,d.PatientId, d.FollowUpDate, d.follow_up_status FROM dbo.crtEthiopiaARTVisit AS d INNER JOIN" & _
" (SELECT MAX(v.Id) AS Id FROM dbo.crtEthiopiaARTVisit AS v INNER JOIN(SELECT     PatientId, MAX(FollowUpDate) AS FollowupDate" & _
" FROM dbo.crtEthiopiaARTVisit WHERE (Deprecated = 0) AND (NOT (follow_up_status IS NULL)) AND (art_start = 1)" & _
" and FollowUpDate <= '" & eDate & "' GROUP BY PatientId) AS l ON v.PatientId = l.PatientId AND v.FollowUpDate = l.FollowupDate" & _
" GROUP BY v.PatientId, v.FollowUpDate, v.Deprecated HAVING(v.Deprecated = 0)) AS lf ON d.Id = lf.Id where d.art_start_date <= '" & eDate & "' and  d.FollowUpDate <= '" & eDate & "' ) as fb on fb.PatientId=n.PatientId group by fb.follow_up_status ;"
       


 mssql = "set nocount on; create table #temp1 ( PatientId int, FollowupDate datetime ); insert into #temp1 SELECT   PatientId, MAX(FollowUpDate) AS FollowupDate FROM dbo.crtEthiopiaARTVisit WHERE (Deprecated = 0) AND (NOT (follow_up_status IS NULL)) AND (art_start = 1)  and FollowUpDate <= '" & sDate & "' GROUP BY PatientId; " & _
"create table #temp2 ( Id int ); insert into #temp2 SELECT     MAX(v.Id) AS Id FROM dbo.crtEthiopiaARTVisit AS v INNER JOIN #temp1 on v.PatientId = #temp1.PatientId AND v.FollowUpDate = #temp1.FollowupDate GROUP BY v.PatientId, v.FollowUpDate, v.Deprecated HAVING v.Deprecated = 0 ;" & _
"create table #temp3 ( PatientId int, FollowupDate datetime ); insert into #temp3 SELECT PatientId, MAX(FollowUpDate) AS FollowupDate  FROM dbo.crtEthiopiaARTVisit WHERE  (Deprecated = 0) AND (NOT (follow_up_status IS NULL)) AND (art_start = 1) and FollowUpDate <= '" & eDate & "' GROUP BY PatientId; " & _
"create table #temp4 ( Id int ); insert into #temp4 SELECT MAX(v.Id) AS Id FROM dbo.crtEthiopiaARTVisit AS v  INNER JOIN #temp3 ON v.PatientId = #temp3.PatientId AND v.FollowUpDate = #temp3.FollowupDate GROUP BY v.PatientId, v.FollowUpDate, v.Deprecated HAVING v.Deprecated = 0; " & _
"select Count(*) as total,TI,new,n.follow_up_status from ( select  f.Id as fid, f.ARTstartedDate, f.follow_up_status, f.PatientId,f.art_dose_end,f.FollowUpDate,c.Id, Case when f.ARTstartedDate <= '" & eDate & "' and  f.ARTstartedDate > '" & sDate & "' then 'N' else 'E' end as new ,dbo.fn_GetTIStatus_2(f.PatientId,  '" & eDate & "', '" & sDate & "') AS TI,case when c.Id is null then 'Not counted' else'counted' end as expr " & _
"from ( SELECT c.Id,c.PatientID,c.art_start_date AS ARTstartedDate, c.[follow_up_status],c.art_dose_end,c.FollowUpDate FROM dbo.Registration AS reg INNER JOIN (SELECT d.Id,d.art_dose_end,d.PatientId, d.FollowUpDate, d.follow_up_status, d.art_start_date, d.next_visit_date FROM dbo.crtEthiopiaARTVisit AS d " & _
"INNER JOIN #temp4 ON d.Id = #temp4.Id) AS c ON c.PatientId = reg.PId where c.follow_up_status >4) as f OUTER APPLY (  SELECT d.Id,d.art_dose_end,d.PatientId, d.FollowUpDate, d.follow_up_status, d.UniqueArtNumber,d.art_start_date, d.next_visit_date FROM dbo.crtEthiopiaARTVisit AS d INNER JOIN #temp2 " & _
"ON d.Id = #temp2.Id where d.follow_up_status > 4   and f.PatientId=d.PatientId and d.art_start_date <= '" & sDate & "' and  d.FollowUpDate <= '" & sDate & "' and  d.art_dose_end >= '" & sDate & "' ) as c where f.ARTstartedDate <= '" & eDate & "' and  f.FollowUpDate <= '" & eDate & "' and " & _
"f.art_dose_end >= '" & eDate & "' and c.Id is null ) as n group by TI,new,n.follow_up_status; " & _
"drop table #temp1; drop table #temp2; drop table #temp3; drop table #temp4 ;"