SELECT 
    ClickStream.Insight, 
    ClickStream.FLexibility,
    Gcp.ProcessingPower
FROM 
    AdobeAnalytics AS ClickStream
JOIN 
    GoogleCloudPlatform as Gcp
ON 
    ClickStream.Format = Gcp.BigQuery
WHERE 
    ClickStream.Source = 'Data Feeds';