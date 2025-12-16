"""
WeWork Data Quality Monitoring System
Automated checks for data integrity and quality issues
Demonstrates script maintenance and debugging capabilities
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class DataQualityChecker:
    def __init__(self):
        self.issues = []
        self.passed_checks = []
        
    def add_issue(self, severity, category, message, details=None):
        """Log a data quality issue"""
        self.issues.append({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'severity': severity,
            'category': category,
            'message': message,
            'details': details
        })
    
    def add_pass(self, check_name):
        """Log a passed check"""
        self.passed_checks.append(check_name)
    
    def check_missing_values(self, df, df_name):
        """Check for missing values in critical columns"""
        print(f"\n🔍 Checking missing values in {df_name}...")
        
        missing = df.isnull().sum()
        missing_pct = (missing / len(df)) * 100
        
        critical_missing = missing[missing > 0]
        
        if len(critical_missing) > 0:
            for col, count in critical_missing.items():
                pct = missing_pct[col]
                if pct > 10:
                    self.add_issue('HIGH', 'Missing Data', 
                                 f"{df_name}: Column '{col}' has {count} missing values ({pct:.1f}%)",
                                 {'column': col, 'count': int(count), 'percentage': float(pct)})
                elif pct > 5:
                    self.add_issue('MEDIUM', 'Missing Data',
                                 f"{df_name}: Column '{col}' has {count} missing values ({pct:.1f}%)",
                                 {'column': col, 'count': int(count), 'percentage': float(pct)})
                else:
                    self.add_issue('LOW', 'Missing Data',
                                 f"{df_name}: Column '{col}' has {count} missing values ({pct:.1f}%)",
                                 {'column': col, 'count': int(count), 'percentage': float(pct)})
        else:
            self.add_pass(f"No missing values in {df_name}")
            print(f"   ✅ No missing values detected")
    
    def check_duplicates(self, df, df_name, subset=None):
        """Check for duplicate records"""
        print(f"\n🔍 Checking duplicates in {df_name}...")
        
        if subset:
            duplicates = df.duplicated(subset=subset).sum()
        else:
            duplicates = df.duplicated().sum()
        
        if duplicates > 0:
            self.add_issue('MEDIUM', 'Duplicates',
                         f"{df_name}: Found {duplicates} duplicate records",
                         {'count': int(duplicates)})
            print(f"   ⚠️  Found {duplicates} duplicate records")
        else:
            self.add_pass(f"No duplicates in {df_name}")
            print(f"   ✅ No duplicates detected")
    
    def check_data_ranges(self, df, df_name, range_checks):
        """Check if numeric values are within expected ranges"""
        print(f"\n🔍 Checking data ranges in {df_name}...")
        
        for col, (min_val, max_val) in range_checks.items():
            if col in df.columns:
                out_of_range = ((df[col] < min_val) | (df[col] > max_val)).sum()
                
                if out_of_range > 0:
                    self.add_issue('MEDIUM', 'Range Validation',
                                 f"{df_name}: {out_of_range} values in '{col}' outside range [{min_val}, {max_val}]",
                                 {'column': col, 'count': int(out_of_range), 'expected_range': [min_val, max_val]})
                    print(f"   ⚠️  Column '{col}': {out_of_range} values out of range")
                else:
                    self.add_pass(f"{df_name}.{col} range check")
                    print(f"   ✅ Column '{col}': All values in valid range")
    
    def check_data_freshness(self, df, df_name, date_column, max_age_days=7):
        """Check if data is recent enough"""
        print(f"\n🔍 Checking data freshness in {df_name}...")
        
        if date_column in df.columns:
            try:
                df[date_column] = pd.to_datetime(df[date_column])
                latest_date = df[date_column].max()
                age_days = (datetime.now() - latest_date).days
                
                if age_days > max_age_days:
                    self.add_issue('HIGH', 'Data Freshness',
                                 f"{df_name}: Latest data is {age_days} days old (threshold: {max_age_days} days)",
                                 {'latest_date': str(latest_date), 'age_days': age_days})
                    print(f"   ⚠️  Data is {age_days} days old (threshold: {max_age_days})")
                else:
                    self.add_pass(f"{df_name} freshness check")
                    print(f"   ✅ Data is fresh ({age_days} days old)")
            except Exception as e:
                self.add_issue('MEDIUM', 'Date Parsing',
                             f"{df_name}: Error parsing date column '{date_column}': {str(e)}")
                print(f"   ⚠️  Could not parse date column")
    
    def check_outliers(self, df, df_name, numeric_columns):
        """Detect statistical outliers using IQR method"""
        print(f"\n🔍 Checking outliers in {df_name}...")
        
        for col in numeric_columns:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                outliers = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()
                outlier_pct = (outliers / len(df)) * 100
                
                if outlier_pct > 5:
                    self.add_issue('LOW', 'Outliers',
                                 f"{df_name}: Column '{col}' has {outliers} outliers ({outlier_pct:.1f}%)",
                                 {'column': col, 'count': int(outliers), 'percentage': float(outlier_pct)})
                    print(f"   ⚠️  Column '{col}': {outliers} outliers detected ({outlier_pct:.1f}%)")
                else:
                    self.add_pass(f"{df_name}.{col} outlier check")
    
    def check_referential_integrity(self, df1, df2, df1_name, df2_name, key_column):
        """Check if foreign key relationships are valid"""
        print(f"\n🔍 Checking referential integrity between {df1_name} and {df2_name}...")
        
        if key_column in df1.columns and key_column in df2.columns:
            orphaned = ~df1[key_column].isin(df2[key_column])
            orphaned_count = orphaned.sum()
            
            if orphaned_count > 0:
                self.add_issue('HIGH', 'Referential Integrity',
                             f"Found {orphaned_count} orphaned records in {df1_name} (missing {key_column} in {df2_name})",
                             {'count': int(orphaned_count), 'key': key_column})
                print(f"   ⚠️  {orphaned_count} orphaned records found")
            else:
                self.add_pass(f"Referential integrity: {df1_name} ↔ {df2_name}")
                print(f"   ✅ Referential integrity maintained")
    
    def generate_report(self):
        """Generate comprehensive quality report"""
        print("\n" + "="*70)
        print("📋 DATA QUALITY REPORT")
        print("="*70)
        
        print(f"\n✅ Passed Checks: {len(self.passed_checks)}")
        print(f"⚠️  Issues Found: {len(self.issues)}")
        
        if len(self.issues) > 0:
            severity_counts = pd.DataFrame(self.issues)['severity'].value_counts()
            print(f"\n📊 Issues by Severity:")
            for severity, count in severity_counts.items():
                emoji = '🔴' if severity == 'HIGH' else '🟡' if severity == 'MEDIUM' else '🟢'
                print(f"   {emoji} {severity}: {count}")
            
            print(f"\n📝 Detailed Issues:")
            for i, issue in enumerate(self.issues, 1):
                emoji = '🔴' if issue['severity'] == 'HIGH' else '🟡' if issue['severity'] == 'MEDIUM' else '🟢'
                print(f"\n   {i}. {emoji} [{issue['severity']}] {issue['category']}")
                print(f"      {issue['message']}")
                print(f"      Time: {issue['timestamp']}")
        else:
            print("\n🎉 All quality checks passed! Data is clean and ready for analysis.")
        
        print("\n" + "="*70)
        
        return {
            'total_checks': len(self.passed_checks) + len(self.issues),
            'passed': len(self.passed_checks),
            'issues': len(self.issues),
            'issues_detail': self.issues
        }

def run_quality_checks():
    """Main function to run all quality checks"""
    checker = DataQualityChecker()
    
    print("🚀 WeWork Data Quality Monitoring System")
    print("="*70)
    
    try:
        # Load datasets
        print("\n📂 Loading datasets...")
        occupancy_df = pd.read_csv('occupancy_data.csv')
        revenue_df = pd.read_csv('revenue_data.csv')
        member_df = pd.read_csv('member_data.csv')
        print("   ✅ All datasets loaded successfully")
        
        # Run checks on occupancy data
        checker.check_missing_values(occupancy_df, 'occupancy_data')
        checker.check_duplicates(occupancy_df, 'occupancy_data', subset=['date', 'location'])
        checker.check_data_ranges(occupancy_df, 'occupancy_data', {
            'occupancy_rate': (0, 100),
            'capacity': (50, 200)
        })
        checker.check_data_freshness(occupancy_df, 'occupancy_data', 'date', max_age_days=7)
        checker.check_outliers(occupancy_df, 'occupancy_data', ['occupancy_rate'])
        
        # Run checks on revenue data
        checker.check_missing_values(revenue_df, 'revenue_data')
        checker.check_duplicates(revenue_df, 'revenue_data')
        checker.check_data_ranges(revenue_df, 'revenue_data', {
            'revenue': (0, 10000000),
            'units_sold': (0, 500)
        })
        
        # Run checks on member data
        checker.check_missing_values(member_df, 'member_data')
        checker.check_duplicates(member_df, 'member_data', subset=['member_id'])
        checker.check_data_freshness(member_df, 'member_data', 'join_date', max_age_days=365)
        
        # Generate final report
        report = checker.generate_report()
        
        # Save report to file
        report_df = pd.DataFrame(checker.issues)
        if len(report_df) > 0:
            report_df.to_csv('data_quality_report.csv', index=False)
            print("\n💾 Quality report saved to: data_quality_report.csv")
        
        return report
        
    except FileNotFoundError as e:
        print(f"\n❌ Error: Required data files not found.")
        print(f"   Please run 'generate_data.py' first to create sample datasets.")
        return None
    except Exception as e:
        print(f"\n❌ Error during quality checks: {str(e)}")
        return None

if __name__ == "__main__":
    report = run_quality_checks()