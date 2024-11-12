# Estruturas das tabelas no supabase

create table
  public.jobs (
    id character varying(40) not null,
    last_updated_date date null,
    body text null,
    title_raw text null,
    url text null,
    sources text[] null,
    language character varying(2) null,
    company bigint null,
    expired date null,
    posted date null,
    skills text[] null,
    title text null,
    title_name text null,
    title_clean text null,
    nation text null,
    occupation text null,
    occupation_name text null,
    specialized_skills text[] null,
    specialized_skills_name text[] null,
    common_skills text[] null,
    common_skills_name text[] null,
    software_skills text[] null,
    software_skills_name text[] null,
    certifications text[] null,
    certifications_name text[] null,
    remote_type text null,
    max_years_experience integer null,
    min_years_experience integer null,
    last_update_import timestamp with time zone null,
    constraint jobs_pkey primary key (id),
    constraint jobs_company_fkey foreign key (company) references company (id),
    constraint jobs_title_fkey foreign key (title) references title_taxonomy (id)
  ) tablespace pg_default;

  create table
  public.skill_2_skill_pt_br (
    id text not null,
    name text null,
    level bigint null,
    subcategory text null,
    subcategory_name text null,
    category text null,
    category_name text null,
    type text null,
    is_software boolean null,
    is_language boolean null,
    wiki_link text null,
    wiki_extract text null,
    description text null,
    description_source text null,
    version text null,
    latest_version boolean null,
    name_pt_br text null,
    description_pt_br text null,
    wiki_extract_pt_br text null,
    constraint skill_2_skill_pt_br_pkey primary key (id),
    constraint skill_2_skill_pt_br_category_fkey foreign key (category) references skill_0_category_pt_bt (id_internal),
    constraint skill_2_skill_pt_br_subcategory_fkey foreign key (subcategory) references skill_1_subcategory_pt_bt (id_internal)
  ) tablespace pg_default;

  create table
  public.title_taxonomy (
    id text not null,
    name text null,
    name_plural text null,
    level integer null,
    band character varying null,
    is_supervisor boolean null,
    version character varying null,
    latest_version boolean null,
    name_plural_pt_br text null default ''::text,
    name_pt_br text null,
    constraint title_taxonomy_pkey primary key (id)
  ) tablespace pg_default;

  create table
  public.company (
    id bigint not null,
    company_name text null,
    constraint company_pkey primary key (id)
  ) tablespace pg_default;

  create table
  public.job_skill (
    job character varying not null,
    skill text null,
    constraint job_skill_job_fkey foreign key (job) references jobs (id) on delete cascade,
    constraint job_skill_skill_fkey foreign key (skill) references skill_2_skill_pt_br (id)
  ) tablespace pg_default;
