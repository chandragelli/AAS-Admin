<#

This Power Shell script is created for performing management operations in Azure Analysis Services cubes.
High-level steps:
 1. Execute SQL DB stored procedure to get meta-data based on the parameters supplied
 2. Get the output of the stored procedure in two data tables(for admin & process operations)
 3. Loop through the rows in admin data table and perform required operation
    1. Check the partition existence in cube
    2. The operation is CREATE and if the partition is not existing, then create the partition
             (OR)
       The operation is DELETE and if the partition is already existing, then delete the partition
 4. Loop through the rows in process data table
    1. Construct PROCESS command for each partition
    2. Chunk the number of partitions by partition count defined in meta-data
    3. Execute the command once it gets a chunk
    4. Repeat the steps until all partitions are processed 
        
#>

#Define parameters
Param (
    [Parameter (Mandatory = $false)]
    [string] $appName = "SCV",
    [Parameter (Mandatory = $false)]
    [string] $cubeName = "SupplyChainViz",
    [Parameter (Mandatory = $false)]
    [string] $tableName = "All",
    [Parameter (Mandatory = $false)]
    [string] $listOnly = "N",
    [Parameter (Mandatory = $false)]
    [int] $retryCount = 3,
    [Parameter (Mandatory = $false)]
    [int] $retryInterval = 60
)
$runId = ""
$stopFlag = "False"
do {
    try{
        #get Power Shell session Id of the invocation
        if($runId -eq "") { $runId = New-Guid }
        Write-Output "(RunId:$runId) Run ID: $runId"

        #test retry
        #Throw "manual error occured"
    
        Write-Output "(RunId:$runId) Parameters passed:-"
        Write-Output "(RunId:$runId) App Name: $appName "
        Write-Output "(RunId:$runId) Cube Name: $cubeNAme "
        Write-Output "(RunId:$runId) Table Name: $tableName "
        Write-Output "(RunId:$runId) List Only: $listOnly"

        #Get Service Principal object defined in Run Book Credentials & other variables
        Write-Output "(RunId:$runId) Get automation SP and variables started at $(Get-Date)"
        $credential = Get-AutomationPSCredential -Name "SRVC_PRL"
        $kvName = Get-AutomationVariable -Name 'KV_NM'
        $kvSecretName = Get-AutomationVariable -Name 'SQL_CONNT_STRG_KV_SCRT_NM'
        $tenantId = Get-AutomationVariable -Name 'TNT_ID'
        Write-Output "(RunId:$runId) Get automation SP and variables ended at $(Get-Date)"

        #Get  SQL Connection string from Key Vault
        Write-Output "(RunId:$runId) Connection string reading from Key Vault started at $(Get-Date)"
        Connect-AzureRMAccount -ServicePrincipal -Credential $Credential -Tenant $tenantId  
        $connString = (Get-AzureKeyVaultSecret -VaultName $kvName -Name $kvSecretName).SecretValueText
        Write-Output "(RunId:$runId) Connection string reading from Key Vault ended at $(Get-Date)"

        $jobName = "AAS_ADMN_PROC"
        $procParams = "exec CTRLDB.Sp_InitializeProcess '$jobName','$cubeName',1,'$appName','$runId'"

        
        #DB Logging - InitializeProcess
        Write-Output "(RunId:$runId) Sp_InitializeProcess started at $(Get-Date)"
        Invoke-Sqlcmd -ConnectionString "$connString" -Query $procParams 
        Write-Output "(RunId:$runId) Sp_InitializeProcess ended at $(Get-Date)"

        $procParams = "exec CTRLDB.PROC_TOAPW_OBJ_ADMN_PROC_WRK '" + $appName + "','" + $cubeName + "','" + $tableName + "','" + $listOnly + "'"

        #Get the list of partitions to be created and/or deleted
        Write-Output "(RunId:$runId) Get list of partitions started at $(Get-Date)"
        $dataSet = Invoke-Sqlcmd -ConnectionString $connString -Query $procParams -As DataSet
        Write-Output "(RunId:$runId) Get list of partitions ended at $(Get-Date)"
        
        #get data tables from data set and set them in appropriate tables
        #if first table has process operation("P"), then there are no admin operations to be performed
         if($dataSet.Tables.Count -gt 0){
         if($dataSet.Tables[0].Rows[0].ADMN_OPR_CD -eq "P"){
             $dataTableProcess = $dataSet.Tables[0]
             $dataTableAdmin = $dataSet.Tables[1]
         }
         else {
            $dataTableAdmin = $dataSet.Tables[0]
            $dataTableProcess = $dataSet.Tables[1]
           }
         }

                    
        #Loop through the list of partitions
        foreach ($row in $dataTableAdmin.Rows)
        { 
          #Assign variables with values from data row
          $iServerName = $row.SRVR_NM
          $iCubeName = $row.SCH_NM
          $iTableName = $row.TBL_NM
          $iPartitionName = $row.PARTN_NM
          $iPartitionDate = $row.PARTN_DT
          $iOperation = $row.ADMN_OPR_CD
          $iTmslCommand = $row.TMSL_CMD

          Write-Output "(RunId:$runId) Server Name: $iServerName"
          Write-Output "(RunId:$runId) Cube Name: $iCubeNAme"
          Write-Output "(RunId:$runId) Table Name: $iTableName"
          Write-Output "(RunId:$runId) Partition Name: $iPartitionName"
          Write-Output "(RunId:$runId) Operation: $iOperation"
          #Write-Output "(RunId:$runId) TMSL Command: $tmslCommand"

          $dmvQuery = "Select [Name] from `$System.TMSCHEMA_PARTITIONS where [Name] = '" + $iPartitionName + "'" 

          #Check to see if the partition is already existing. This will be checked for "True" in case of deletion and "False" for creation. 
          #Just to ensure partition is existing before deletion and it's not getting recreated if it's already existing
          Write-Output "(RunId:$runId) Partition existence check started at $(Get-Date)"
          $AsCmdResponse = Invoke-AsCmd -Server $iServerName -Database $iCubeName -ServicePrincipal -Credential $credential -Query $dmvQuery

          #Check AsCmd response for failure, if so Throw the response as Exception
          $isAsCmdError = $AsCmdResponse.contains("Error")
          if($isAsCmdError) {
          Throw $AsCmdResponse
          }

          $isPartitionExisting = $AsCmdResponse.contains($iPartitionName)
          Write-Output "(RunId:$runId) Partition existence check ended at $(Get-Date)"

          Write-Output "(RunId:$runId) Is partition already existing? $isPartitionExisting"

          #DELETE partition command
          if($iOperation -eq "D" -and $isPartitionExisting)
          {
             Write-Output "(RunId:$runId) Partition deletion started at $(Get-Date)"
             $AsCmdResponse = Invoke-AsCmd -Server $iServerName -ServicePrincipal -Credential $credential -Query $iTmslCommand

             #Check AsCmd response for failure, if so Throw the response as Exception
             $isAsCmdError = $AsCmdResponse.contains("Error")
             if($isAsCmdError) {
              Throw $AsCmdResponse
             }
             Write-Output "(RunId:$runId) Partition deletion ended at $(Get-Date)" 

             #Restate the partition start date(PARTN_STRT_DT) to next calendar date in case of delete
             $procParams = "UPDATE CTRLDB.TODRD_OBJ_DAT_RTN_DEF SET PARTN_STRT_DT = dateadd(day,1,'$iPartitionDate') WHERE SRVR_NM='$iServerName' and OBJ_NM = '$itableName' and SCH_NM = '$iCubeName'"
             Write-Output "(RunId:$runId) Partition Start Date restatement started at $(Get-Date)"
             Invoke-Sqlcmd -ConnectionString $connString -Query $procParams 
             Write-Output "(RunId:$runId) Partition Start Date restatement ended at $(Get-Date)"
          }
          #CREATE partition command
          if($iOperation -eq "C" -and -not $isPartitionExisting)
          {
             Write-Output "(RunId:$runId) Partition creation started at $(Get-Date)"
             $AsCmdResponse = Invoke-AsCmd -Server $iServerName -ServicePrincipal -Credential $credential -Query $iTmslCommand

             #Check AsCmd response for failure, if so Throw the response as Exception
             $isAsCmdError = $AsCmdResponse.contains("Error")
             if($isAsCmdError) {
              Throw $AsCmdResponse
             }
             Write-Output "(RunId:$runId) Partition creation ended at $(Get-Date)"
          }
          
        }

         
         $processPartitionsCount = $dataTableProcess.Rows.Count
         #Get number of process partitions threshold from meta-data if there are any partitions to be processed
         if($processPartitionsCount -gt 0)
          {
          
             $procParams = "select cast(outputValue as int) PARTN_CNT from ctrldb.SCVDomainLookup where InputParameter = 'PartitionProcessLimit' and DomainParameter  = 'AAS-AdhocProcess'"
             Write-Output "(RunId:$runId) Get partition process count started at $(Get-Date)"
             $QueryResult = Invoke-Sqlcmd -ConnectionString $connString -Query $procParams
             $partitionCount = $QueryResult  | Select-object  -ExpandProperty  PARTN_CNT  
             if($partitionCount -lt 1){
               $partitionCount = $processPartitionsCount
             }
             Write-Output "(RunId:$runId) Get partition process count ended at $(Get-Date)"

             #preset process command 
             $processTMSLCommand = '{"refresh": { "type": "full","objects": [ '
             $processFlag = "False"
          }

         $i = 1
         foreach ($row in $dataTableProcess)
         { 
              #Assign variables with values from data row
              $iServerName = $row.SRVR_NM
              $iCubeNAme = $row.SCH_NM
              $iTableName = $row.TBL_NM
              $iPartitionName = $row.PARTN_NM
              $iPartitionDate = $row.PARTN_DT
              $iOperation = $row.ADMN_OPR_CD
             
              Write-Output "(RunId:$runId) Server Name: $iServerName"
              Write-Output "(RunId:$runId) Cube Name: $iCubeNAme"
              Write-Output "(RunId:$runId) Table Name: $iTableName"
              Write-Output "(RunId:$runId) Partition Name: $iPartitionName"
              Write-Output "(RunId:$runId) Operation: $iOperation"

              $dmvQuery = "Select [Name] from `$System.TMSCHEMA_PARTITIONS where [Name] = '" + $iPartitionName + "'" 

              #Write-Output $dmvQuery

              #Check to see if the partition is already existing. This will be checked for "True" in case of deletion and "False" for creation. 
              #Just to ensure partition is existing before deletion and it's not getting recreated if it's already existing
              Write-Output "(RunId:$runId) Partition existence check started at $(Get-Date)"
              $AsCmdResponse = Invoke-AsCmd -Server $iServerName -Database $iCubeName -ServicePrincipal -Credential $credential -Query $dmvQuery

              #Check AsCmd response for failure, if so Throw the response as Exception
              $isAsCmdError = $AsCmdResponse.contains("Error")
              if($isAsCmdError) {
              Throw $AsCmdResponse
              }

              $isPartitionExisting = $AsCmdResponse.contains($iPartitionName)
              Write-Output "(RunId:$runId) Partition existence check ended at $(Get-Date)"

              Write-Output "(RunId:$runId) Is partition already existing? $isPartitionExisting"

              #PROCESS partition command(Construct the full JSON for all partitions to be processed and then run the command as a whole for parallel processing
              if($isPartitionExisting){
                $processFlag = "True"          
                $processTMSLCommand =  "$processTMSLCommand {`"database`":`"$iCubeNAme`", `"table`": `"$iTableName`",`"partition`": `"$iPartitionName`"},"
                $partitionList = "$partitionList '$iPartitionName',"
              }

             # Run process partition command if it gets a chunk
             if($i%$partitionCount -eq 0 -or ($processPartitionsCount -lt $partitionCount -and $i -eq $dataTableProcess.Rows.Count) -and ($processFlag -eq "True"))
              {
                 $processTMSLCommand = $processTMSLCommand.Substring(0,$processTMSLCommand.Length-1) + ']}}'
                 $partitionList = $partitionList.Substring(0,$partitionList.Length-1) 
                 Write-Output "(RunId:$runId) Partition Process JSON: $processTMSLCommand"
                 Write-Output "(RunId:$runId) Partition process started at $(Get-Date)"
                 $AsCmdResponse = Invoke-AsCmd -Server $iServerName -ServicePrincipal -Credential $credential -Query $processTMSLCommand

                 #Check AsCmd response for failure, if so Throw the response as Exception
                 $isAsCmdError = $AsCmdResponse.contains("Error")
                 if($isAsCmdError) {
                  Throw $AsCmdResponse
                 }
                 Write-Output "(RunId:$runId) Partition process ended at $(Get-Date)" 

                 #disable flag in DB table for paritions that are processed as a "checkpoint"
                $procParams = "UPDATE CTRLDB.TOAPW_OBJ_ADMN_PROC_WRK SET ACTV_REC_IND = 'N'  WHERE PARTN_NM IN ($partitionList) AND APP_NM = '$appName' AND SRVR_NM = '$iServerName'  AND SCH_NM = '$iCubeNAme' AND  ADMN_OPR_CD = 'P'"
                Write-Output $procParams
                Write-Output "(RunId:$runId) Disable partition process flag(checkpoint) started at $(Get-Date)"
                Invoke-Sqlcmd -ConnectionString $connString -Query $procParams 
                Write-Output "(RunId:$runId) Disable partition process flag(checkpoint) ended at $(Get-Date)"

                 #reset variables
                 $processFlag = "False"
                 $processTMSLCommand = '{"refresh": { "type": "full","objects": [ '
                 $partitionList = ""

              }
              $i = $i + 1
              $processPartitionsCount = $processPartitionsCount - 1
         }

        $procParams = "exec CTRLDB.Sp_CleanupProcess '$jobName','$cubeName',1,NULL,NULL,NULL,'$appName',NULL"

        #DB Logging - Cleanup Process
        Write-Output "(RunId:$runId) Sp_CleanupProcess started at $(Get-Date)"
        Invoke-Sqlcmd -ConnectionString $connString -Query $procParams 
        Write-Output "(RunId:$runId) Sp_CleanupProces ended at $(Get-Date)"

        $stopFlag = "True"
    }
    catch
    {
        $retryCount = $retryCount - 1
        Write-Output "(RunId:$runId) An error occurred at $(Get-Date)"
        Write-Output $error[0].Exception.Message
        Write-Output $error[0].Exception.StackTrace
   
        if($retryCount -eq 0)
        {
          $stopFlag = "True"
          Write-Output "(RunId:$runId) Maximum number($retryCount) of retry attempts has exceeded!"
          #DB Logging - Cleanup Process
          $procParams = "exec CTRLDB.Sp_CleanupProcess '$jobName','$cubeName',1,NULL,9999,$error[0].Exception.Message,'$appName',NULL"
          Write-Output "(RunId:$runId) Sp_CleanupProcess(Error) started at $(Get-Date)"
          Invoke-Sqlcmd -ConnectionString $connString -Query $procParams 
          Write-Output "(RunId:$runId) Sp_CleanupProces(Error) ended at $(Get-Date)"
        }
        else
        {
          Write-Output "(RunId:$runId) Retry interval started at $(Get-Date)"
          Start-Sleep -Seconds $retryInterval
          Write-Output "(RunId:$runId) Retry interval ended at $(Get-Date)"
          Write-Output "(RunId:$runId) Script is getting retried with remaining retry attempts: $retryCount"
          $dataSet = ""
          $dataTableAdmin = ""
          $dataTableProcess = ""
        }
    }
}
While ($stopFlag -eq "False")

#Throw the exception after all retry attempts are done 
if($retryCount -eq 0){
    Throw $error[0].Exception.Message
 }         
