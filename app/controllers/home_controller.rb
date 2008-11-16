class HomeController < ApplicationController
  skip_before_filter :require_activation
  def index
    @body = "home"
    @topics = Topic.find_recent
    @members = Person.find_recent
    @upcoming_categories = UpcomingCategory.find(:all)
    @music = UpcomingCategory.find(:all, :conditions => ["name = ?", "Media"])
    @date = 10.day.from_now.strftime("%Y-%m-%d")
    if (not params[:search_text].blank? and not params[:location].blank?) then
      @search_conditions = ["search_text = ? and (woeid in (select woeid from geo.places where text = ? limit 1)) and start_date < ?", params[:search_text], params[:location], @date]
    elsif (not params[:location].blank?) then
      @search_conditions = ["(woeid in (select woeid from geo.places where text = ? limit 1)) and start_date < ?", params[:location], @date]
    else
      @search_conditions = ["(woeid in (select woeid from geo.places where text = ? limit 1)) and start_date < ?", "San Francisco", @date]
    end
    @total_upcoming_events = UpcomingEvent.find(:all, :conditions => @search_conditions).length
    @upcoming_events = UpcomingEvent.paginate(:all, :page => params[:page], :limit => 3, :total_entries => @total_upcoming_events, :conditions => @search_conditions)
    if logged_in?
      @feed = current_person.feed
      @some_contacts = current_person.some_contacts
      @requested_contacts = current_person.requested_contacts
    else
      @feed = Activity.global_feed
    end    
    respond_to do |format|
      format.html
      format.atom
    end  
  end
end
