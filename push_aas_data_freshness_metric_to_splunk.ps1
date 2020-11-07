<#
    This Power Shell script is created to retrieve Data Freshness metric from Order & Shipment cubes and push it to Splunk
    Data alert can then be created out of Splunk based on the difference between the current and cube metric value
#>
$stopFlag = "False"
$retry_count = Get-AutomationVariable -Name "RTRY_CNT"
$retry_interval_factor = Get-AutomationVariable -Name "RTRY_INTRVL_FCTR"
$attempt = 1
do {
    try{
            # get values for the variables from automation account variables collection
            $credential = Get-AutomationPSCredential -Name "SRVC_PRL"
            $aas_server_name =  Get-AutomationVariable -Name "AAS_SRVR_NM"
            $splunk_server_url =  Get-AutomationVariable -Name "SPLUNK_SRVR_URL"
            $splunk_auth_key = Get-AutomationVariable -Name "SPLUNK_AUTH_KEY"
            $success_message_format = Get-AutomationVariable -Name "SUCCESS_MSG_FRMT"
            $warning_message_format = Get-AutomationVariable -Name "WARNING_MSG_FRMT"
            $error_message_format = Get-AutomationVariable -Name "ERROR_MSG_FRMT"
            $success_message_format = $success_message_format.replace("server_name_var",$aas_server_name)
            $warning_message_format = $warning_message_format.replace("server_name_var",$aas_server_name)
            $error_message_format = $error_message_format.replace("server_name_var",$aas_server_name)
            
            # allow the use of self-signed SSL certificates
            [System.Net.ServicePointManager]::ServerCertificateValidationCallback = { $True }

            $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
            $headers.Add("Authorization",  $splunk_auth_key )

            # define the cube name and MDX query to get "data freshness" metric 
            $cube_name = "SupplyChainViz"
            $mdx_query = " SELECT [Measures].[Last Updated] ON COLUMNS FROM [SupplyChainViz] CELL PROPERTIES VALUE"

            # execute the MDX query and extract result value from XML
            Write-Output "Get data freshness metric for Order Cube started at $(Get-Date)"
            [xml]$xml  = Invoke-AsCmd -Server $aas_server_name -Database $cube_name -ServicePrincipal -Credential $credential -Query $mdx_query
            $latest_order_datetime = $xml.return.root.CellData.Cell.Value.'#text'
            Write-Output "Get data freshness metric for Order Cube ended at $(Get-Date)"

            $date = Get-Date -Format o | ForEach-Object { $_ -replace ":", "." }
            $body = $success_message_format.replace("cube_name_var",$cube_name)
            $body = $body.replace("metric_value_var",$latest_order_datetime)
            $body = $body.replace("date_var",$date)

            Write-Output "REST method for Order cube started at $(Get-Date)"
            $response = Invoke-RestMethod -Uri $splunk_server_url -Method Post -Headers $headers  -Body $body
            Write-Output "REST method for Order cube ended at $(Get-Date)"

            if($response.text -ne "Success") {
                Throw "Error occured in the REST method when posting the message for $cube_name"
            }
            else {
                Write-Output "REST method successfully posted the message for $cube_name"
            }
          
            # define the cube name and MDX query to get "data freshness" metric 
            $cube_name = "SupplyChainVizShipment"

            $markets = @("Canada", "European Union", "United Kingdom","United States","Japan")
            foreach ($market in $markets) {
                # execute the MDX query and extract result value from XML
                Write-Output "Get data freshness metric for Shipment cube($market) started at $(Get-Date)"
                $mdx_query = "SELECT  [Measures].[Last Updated Pack Time]  ON COLUMNS FROM [SupplyChainVizShipment] WHERE  [Country].[Market].&[$market]  CELL PROPERTIES VALUE"
                [xml]$xml  = Invoke-AsCmd -Server $aas_server_name -Database $cube_name -ServicePrincipal -Credential $credential -Query $mdx_query
                $latest_pack_datetime = $xml.return.root.CellData.Cell.Value.'#text'
                Write-Output "Get data freshness metric for Shipment cube($market) ended at $(Get-Date)"

                if($null -ne $latest_pack_datetime) {  
                    $date = Get-Date -Format o | ForEach-Object { $_ -replace ":", "." }
                    $body = $success_message_format.replace("cube_name_var",$cube_name)
                    $body = $body.replace("metric_value_var",$latest_pack_datetime)
                    $body = $body.replace("date_var",$date)
                    $body = $body.replace('"data": {','"data": { "market":"' + $market + '",')
                    Write-Output $body

                    Write-Output "REST method for Shipment cube($market) started at $(Get-Date)"
                    $response = Invoke-RestMethod -Uri $splunk_server_url -Method Post -Headers $headers  -Body $body
                    Write-Output "REST method for Shipment cube($market) ended at $(Get-Date)"

                    if($response.text -ne "Success") {
                        Throw "Error occured in the REST method when posting the message for $cube_name $market"
                    }
                    else {
                        Write-Output "REST method successfully posted the message for $cube_name $market"
                        }
                }

            }
            $stopFlag = "True"
        }
    catch{
            $retry_count = $retry_count - 1
            $error_message = $error[0].Exception.Message
            Write-Output "An error occurred at $(Get-Date)"
            Write-Output $error_message 
            Write-Output $error[0].Exception.StackTrace
    
            if($retry_count -eq 0){
                    #post WARNING message to Splunk
                    $date = Get-Date -Format o | ForEach-Object { $_ -replace ":", "." }
                    $body = $error_message_format.replace("error_message_var",$error_message)
                    $body = $body.replace("date_var",$date)
                    $body = $body.replace("cube_name_var",$cube_name)
                    $response = Invoke-RestMethod -Uri $splunk_server_url -Method Post -Headers $headers  -Body $body

                    #set the flag for script termination
                    $stopFlag = "True"
                    Write-Output "Maximum number of retry attempts has exceeded!"
                }
            else{
                    #post ERROR message to Splunk
                    $date = Get-Date -Format o | ForEach-Object { $_ -replace ":", "." }
                    $body = $warning_message_format.replace("error_message_var",$error_message)
                    $body = $body.replace("date_var",$date)
                    $body = $body.replace("cube_name_var",$cube_name)
                    $response = Invoke-RestMethod -Uri $splunk_server_url -Method Post -Headers $headers  -Body $body

                    $retry_interval = ($attempt * $attempt * $retry_interval_factor)  * 60
                    Write-Output "Retry interval seconds: $retry_interval"
                    Write-Output "Retry interval started at $(Get-Date)"
                    Start-Sleep -Seconds $retry_interval
                    Write-Output "Retry interval ended at $(Get-Date)"
                    Write-Output "Remaining retry attempts: $retry_count"

                    $attempt = $attempt + 1

                }
    }
}
While ($stopFlag -eq "False")
