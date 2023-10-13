def dmt():
    return '''SELECT 
       [Selling Division], 
       [COUNTRY_GROUP] 'Area',
       [Stryker Group Region],
       [Region],
       [Country],
       p.[CatalogNumber],
       p.[Business Sector],
       p.[Franchise],
       p.[Product Line],
       p.[IBP Level 5],
       [SALES_DATE],
       sum(xx_l1_asp_rev) as [`L0 ASP Final Rev],
       SUM([EBS_SH_REQ_QTY_RD]) "`Act Orders Rev",
       sum([SDATA10]) "Act Orders Rev Val",
       sum(s."`L2 DF Final Rev") as [L2 DF Final Rev],
       sum(s.xx_l3_fstat_rev) as [L2 Stat Final Rev],
       sum(XX_FINAL_DPFCST) as [`Fcst DF Final Rev],
       sum(XX_MODREV_OVRD) as [`Fcst Stat Final Rev],
       sum(S_PRED_OLD) as [`Fcst Stat Prelim Rev],
       sum(XX_FINDP_FCST) as [Fcst DF Final Rev Val],
       'DMT' as [Source]

FROM [Envision].[Fact_Sales] s

JOIN [Envision].[dim_demantraproducts] p
ON s.item_skey = p.item_skey

JOIN [Envision].[Dim_DemantraLocation] l
ON s.Location_sKey = l.Location_skey

JOIN [Envision].[Dim_MDPMatrix] m
ON s.MDP_Key = m.MDP_Key
 
WHERE 
       ([SALES_DATE] BETWEEN DATEADD(yy, DATEDIFF(yy, 0, GETDATE())-3, 0) AND DATEADD(yy, DATEDIFF(yy, 0, GETDATE()) + 3, -1)) AND
       p.Franchise IN (?,?,?,?,?,?) AND
       [Country] in (?)

    GROUP BY
        [Selling Division], 
       [COUNTRY_GROUP],
       [Stryker Group Region],
       [Region],
       [Country],
       p.[Business Sector],
       p.[Franchise],
       p.[IBP Level 5],
       p.[Product Line],
       [SALES_DATE],
       p.[CatalogNumber]'''
       
def cld(): 
    return '''SELECT 
       [SellingDivision] as [Selling Division], 
       [COUNTRY_GROUP] 'Area',
       [StrykerGroupRegion] as [Stryker Group Region],
       [Region],
       [Country],
          p.[CatalogNumber],
          p.[BusinessSector] as [Business Sector],
          p.[Franchise],
          p.[ProductLine] as [Product Line],
          p.[IBPLevel5] as [IBP Level 5],
          [SALES_DATE],
		  sum([L0_ASP_Final_Rev]) [`L0 ASP Final Rev],
          SUM([Act_Orders_Rev]) "`Act Orders Rev",
          sum([Act_Orders_Rev_Val]) "Act Orders Rev Val",
          sum(s.[L2_DF_Final_Rev]) as [L2 DF Final Rev],
          sum(s.[L2_Stat_Final_Rev]) as [L2 Stat Final Rev],
          sum(Fcst_DF_Final_Rev) as [`Fcst DF Final Rev],
       sum(Fcst_Stat_Final_Rev) as [`Fcst Stat Final Rev],
       sum(Fcst_Stat_Prelim_Rev) as [`Fcst Stat Prelim Rev],
       sum(Fcst_DF_Final_Rev_Val) as [Fcst DF Final Rev Val],
       'DMTCLD' as [Source]
         
       
FROM [Envision].[Demantra_CLD_Fact_Sales] s

JOIN [Envision].[DIM_Demantra_CLD_demantraproducts] p
ON s.item_skey = p.item_skey
        
JOIN [Envision].[DIM_Demantra_CLD_DemantraLocation] l
ON s.Location_sKey = l.Location_skey
       
JOIN [Envision].[Dim_DEMANTRA_CLD_MDP_Matrix] m
ON s.MDP_Key = m.MDP_Key

WHERE 
        ([SALES_DATE] BETWEEN DATEADD(yy, DATEDIFF(yy, 0, GETDATE())-3, 0) AND DATEADD(yy, DATEDIFF(yy, 0, GETDATE()) + 3, -1)) AND
        p.Franchise IN (?,?,?,?,?,?) AND
        [Country] in (?)
          
       GROUP BY
       [SellingDivision], 
       [COUNTRY_GROUP],
       [StrykerGroupRegion],
       [Region],
       [Country],
       p.[BusinessSector],
       p.[Franchise],
          p.[IBPLevel5],
          p.[ProductLine],
       [SALES_DATE],
       p.[CatalogNumber]'''