EXEC [bronze].[load_bronze];

CREATE OR ALTER PROCEDURE  bronze.load_bronze  AS
BEGIN
    BEGIN TRY
       PRINT '=============================';
       PRINT 'LOADING BRONZE LAYER';
       PRINT '=============================';

       PRINT '--------------------------------------------------------------------';
       PRINT 'LOADING CRM TABLES';
       PRINT '--------------------------------------------------------------------';
       
           PRINT '>>>TRUNCATING TABLE [bronze].[crm_cust_info]';
        TRUNCATE TABLE [bronze].[crm_cust_info];
          PRINT 'INSERTIN DATA INTO TABLE [bronze].[crm_cust_info]';
        BULK INSERT [bronze].[crm_cust_info]
        FROM 'C:\sql-datawarehouse-csv\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );


        PRINT '>>>TRUNCATING TABLE  [bronze].[crm_prd_info]';
        TRUNCATE TABLE [bronze].[crm_prd_info];
        PRINT 'INSERTIN DATA INTO TABLE [bronze].[crm_prd_info]';
        BULK INSERT [bronze].[crm_prd_info]
        FROM 'C:\sql-datawarehouse-csv\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

       

        PRINT '>>>TRUNCATING TABLE [bronze].[crm_sales_details]';
        TRUNCATE TABLE [bronze].[crm_sales_details];
        PRINT 'INSERTIN DATA INTO TABLE [bronze].[crm_sales_details]';
        BULK INSERT [bronze].[crm_sales_details]
        FROM 'C:\sql-datawarehouse-csv\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

      

       PRINT '--------------------------------------------------------------------';
       PRINT 'LOADING ERP TABLES';
       PRINT '--------------------------------------------------------------------';

        PRINT '>>>TRUNCATING TABLE [bronze].[erp_cust_az12]';
        TRUNCATE TABLE [bronze].[erp_cust_az12];
        PRINT 'INSERTIN DATA INTO TABLE [bronze].[erp_cust_az12]';
        BULK INSERT [bronze].[erp_cust_az12]
        FROM 'C:\sql-datawarehouse-csv\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

      
        PRINT '>>>TRUNCATING TABLE [bronze].[erp_loc_a101]';
        TRUNCATE TABLE [bronze].[erp_loc_a101];
        PRINT 'INSERTIN DATA INTO TABLE [bronze].[erp_loc_a101]';
        BULK INSERT [bronze].[erp_loc_a101]
        FROM 'C:\sql-datawarehouse-csv\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );


           PRINT '>>>TRUNCATING TABLE [bronze].[erp_px_cat_g1v2]';
        TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];
        PRINT 'INSERTIN DATA INTO TABLE [bronze].[erp_px_cat_g1v2]';
        BULK INSERT [bronze].[erp_px_cat_g1v2]
        FROM 'C:\sql-datawarehouse-csv\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        END TRY
        BEGIN CATCH
        PRINT '----------------------------------------------------------------------------------'
        PRINT ' ERROR OCCURED DURING LOADING';
        PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
        PRINT 'ERROR MESSAGE' + CAST( ERROR_NUMBER() AS VARCHAR) 
        PRINT '----------------------------------------------------------------------------------';
        END CATCH

END
